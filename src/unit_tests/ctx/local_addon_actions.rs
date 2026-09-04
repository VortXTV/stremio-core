use crate::constants::{PROFILE_STORAGE_KEY, STREAMS_STORAGE_KEY};
use crate::models::ctx::Ctx;
use crate::runtime::msg::{Action, ActionCtx};
use crate::runtime::{
    Env, EnvError, EnvFutureExt, Runtime, RuntimeAction, RuntimeEvent, TryEnvFuture,
};
use crate::types::addon::{Descriptor, Manifest};
use crate::types::api::{
    APIResult, AuthRequest, AuthResponse, CollectionResponse, LibraryItemsResponse, SuccessResponse,
};
use crate::types::events::DismissedEventsBucket;
use crate::types::library::LibraryBucket;
use crate::types::notifications::NotificationsBucket;
use crate::types::profile::{Auth, AuthKey, Profile, User};
use crate::types::resource::{Stream, StreamBehaviorHints, StreamSource};
use crate::types::search_history::SearchHistoryBucket;
use crate::types::server_urls::ServerUrlsBucket;
use crate::types::streams::{StreamsBucket, StreamsItem, StreamsItemKey};
use crate::types::True;
use crate::unit_tests::{
    default_fetch_handler, Request, TestEnv, EVENTS, FETCH_HANDLER, REQUESTS, STORAGE,
};
use futures::channel::{mpsc::Receiver, oneshot};
use futures::{future, TryFutureExt};
use semver::Version;
use std::any::Any;
use std::sync::{Arc, Mutex, RwLock};
use stremio_derive::Model;
use url::Url;

#[derive(Model, Clone, Default)]
#[model(TestEnv)]
struct TestModel {
    ctx: Ctx,
}

fn addon(url: &str, version: u64) -> Descriptor {
    Descriptor {
        manifest: Manifest {
            id: "addon-id".to_owned(),
            version: Version::new(0, 0, version),
            name: "Addon".to_owned(),
            contact_email: None,
            description: None,
            logo: None,
            background: None,
            types: vec![],
            resources: vec![],
            id_prefixes: None,
            catalogs: vec![],
            addon_catalogs: vec![],
            behavior_hints: Default::default(),
        },
        transport_url: Url::parse(url).unwrap(),
        flags: Default::default(),
    }
}

fn authenticated_profile(addons: Vec<Descriptor>) -> Profile {
    Profile {
        auth: Some(Auth {
            key: AuthKey("auth_key".to_owned()),
            user: User {
                id: "user_id".into(),
                email: "user@example.com".to_owned(),
                ..Default::default()
            },
        }),
        addons,
        ..Default::default()
    }
}

fn stream_item(addon: &Descriptor) -> StreamsItem {
    StreamsItem {
        stream: Stream {
            source: StreamSource::Url {
                url: Url::parse("https://stream.example/video").unwrap(),
            },
            name: None,
            description: None,
            thumbnail: None,
            subtitles: vec![],
            behavior_hints: StreamBehaviorHints::default(),
        },
        r#type: "movie".to_owned(),
        meta_id: "tt123".to_owned(),
        video_id: "tt123:1:0".to_owned(),
        meta_transport_url: addon.transport_url.to_owned(),
        stream_transport_url: addon.transport_url.to_owned(),
        state: None,
        mtime: TestEnv::now(),
    }
}

fn new_runtime(
    profile: Profile,
    streams: StreamsBucket,
) -> (
    Runtime<TestEnv, TestModel>,
    Receiver<crate::runtime::RuntimeEvent<TestEnv, TestModel>>,
) {
    Runtime::<TestEnv, _>::new(
        TestModel {
            ctx: Ctx::new(
                profile,
                LibraryBucket::default(),
                streams,
                ServerUrlsBucket::new::<TestEnv>(None),
                NotificationsBucket::new::<TestEnv>(None, vec![]),
                SearchHistoryBucket::default(),
                DismissedEventsBucket::default(),
            ),
        },
        vec![],
        1000,
    )
}

fn api_success(request: Request) -> TryEnvFuture<Box<dyn Any + Send>> {
    match request {
        Request { url, method, .. }
            if url == "https://api.strem.io/api/addonCollectionSet" && method == "POST" =>
        {
            future::ok(
                Box::new(APIResult::Ok(SuccessResponse { success: True {} }))
                    as Box<dyn Any + Send>,
            )
            .boxed_env()
        }
        request => default_fetch_handler(request),
    }
}

#[test]
fn actionctx_local_addon_actions_deserialize() {
    let descriptor = serde_json::json!({
        "transportUrl": "https://addon.example/manifest.json",
        "manifest": {
            "id": "addon-id", "version": "0.0.1", "name": "Addon",
            "contactEmail": null, "description": null, "logo": null, "background": null,
            "types": [], "resources": [], "idPrefixes": null, "catalogs": [],
            "addonCatalogs": [], "behaviorHints": {
                "adult": false, "p2p": false, "configurable": false, "configurationRequired": false
            }
        },
        "flags": { "official": false, "protected": false }
    });

    for action in [
        "InstallAddonLocal",
        "UpgradeAddonLocal",
        "UninstallAddonLocal",
    ] {
        let parsed = serde_json::from_value::<ActionCtx>(serde_json::json!({
            "action": action,
            "args": descriptor.clone(),
        }))
        .unwrap();
        assert!(matches!(
            parsed,
            ActionCtx::InstallAddonLocal(_)
                | ActionCtx::UpgradeAddonLocal(_)
                | ActionCtx::UninstallAddonLocal(_)
        ));
    }

    let parsed = serde_json::from_value::<ActionCtx>(serde_json::json!({
        "action": "ReplaceAddonLocal",
        "args": { "old": descriptor.clone(), "new": descriptor },
    }))
    .unwrap();
    assert!(matches!(parsed, ActionCtx::ReplaceAddonLocal { .. }));

    let parsed = serde_json::from_value::<ActionCtx>(serde_json::json!({
        "action": "ReplaceAddonProjectionLocal",
        "args": [descriptor],
    }))
    .unwrap();
    assert!(matches!(parsed, ActionCtx::ReplaceAddonProjectionLocal(_)));
}

#[test]
fn actionctx_local_mutations_persist_and_never_push_authenticated_collection() {
    let initial = addon("https://addon.example/manifest.json", 1);
    let upgraded = addon("https://addon.example/manifest.json", 2);
    let mut streams = StreamsBucket::default();
    streams.items.insert(
        StreamsItemKey {
            meta_id: "tt123".to_owned(),
            video_id: "tt123:1:0".to_owned(),
        },
        stream_item(&initial),
    );
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, _rx) = new_runtime(authenticated_profile(vec![]), streams);

    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddonLocal(initial)),
        });
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::UpgradeAddonLocal(upgraded.clone())),
        });
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::UninstallAddonLocal(upgraded)),
        });
    });

    assert!(runtime.model().unwrap().ctx.profile.addons.is_empty());
    assert!(runtime.model().unwrap().ctx.streams.items.is_empty());
    assert!(REQUESTS.read().unwrap().is_empty());
    assert!(STORAGE
        .read()
        .unwrap()
        .get(PROFILE_STORAGE_KEY)
        .is_some_and(|data| serde_json::from_str::<Profile>(data)
            .unwrap()
            .addons
            .is_empty()));
}

#[test]
fn actionctx_local_install_emits_the_normal_addon_event_without_api_write() {
    let addon = addon("https://addon.example/manifest.json", 1);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, rx) = new_runtime(authenticated_profile(vec![]), StreamsBucket::default());
    let runtime = Arc::new(RwLock::new(runtime));

    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        let addon = addon.clone();
        move || {
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::InstallAddonLocal(addon)),
            });
        }
    });

    assert!(EVENTS.read().unwrap().iter().any(|event| {
        matches!(
            event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap(),
            RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonInstalled {
                transport_url,
                id,
            }) if transport_url == &addon.transport_url && id == &addon.manifest.id
        )
    }));
    assert!(REQUESTS.read().unwrap().is_empty());
}

#[test]
fn actionctx_normal_and_local_replace_have_one_or_zero_collection_writes() {
    let old = addon("https://old.example/manifest.json", 1);
    let new = addon("https://new.example/manifest.json", 2);
    let peer = addon("https://peer.example/manifest.json", 1);
    let mut streams = StreamsBucket::default();
    streams.items.insert(
        StreamsItemKey {
            meta_id: "tt123".to_owned(),
            video_id: "tt123:1:0".to_owned(),
        },
        stream_item(&old),
    );

    let _env_mutex = TestEnv::reset().unwrap();
    *FETCH_HANDLER.write().unwrap() = Box::new(api_success);
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![old.clone(), peer.clone()]),
        streams.clone(),
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddon {
                old: old.clone(),
                new: new.clone(),
            }),
        });
    });
    assert_eq!(
        runtime.model().unwrap().ctx.profile.addons,
        vec![new.clone(), peer.clone()]
    );
    assert!(runtime.model().unwrap().ctx.streams.items.is_empty());
    assert_eq!(REQUESTS.read().unwrap().len(), 1);
    assert_eq!(
        REQUESTS.read().unwrap()[0].body,
        format!(
            "{{\"type\":\"AddonCollectionSet\",\"authKey\":\"auth_key\",\"addons\":[{},{}]}}",
            serde_json::to_string(&new).unwrap(),
            serde_json::to_string(&peer).unwrap(),
        ),
        "the one remote request must contain the final replacement collection in-place, never an intermediate removal snapshot"
    );
    assert!(STORAGE.read().unwrap().contains_key(PROFILE_STORAGE_KEY));

    drop(_env_mutex);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![old.clone(), peer.clone()]),
        streams,
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonLocal {
                old,
                new: new.clone(),
            }),
        });
    });
    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![new, peer]);
    assert!(runtime.model().unwrap().ctx.streams.items.is_empty());
    assert!(REQUESTS.read().unwrap().is_empty());
}

#[test]
fn actionctx_normal_upgrade_still_pushes_authenticated_collection() {
    let installed = addon("https://addon.example/manifest.json", 1);
    let upgraded = addon("https://addon.example/manifest.json", 2);
    let _env_mutex = TestEnv::reset().unwrap();
    *FETCH_HANDLER.write().unwrap() = Box::new(api_success);
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![installed]),
        StreamsBucket::default(),
    );

    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::UpgradeAddon(upgraded.clone())),
        });
    });

    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![upgraded]);
    assert_eq!(REQUESTS.read().unwrap().len(), 1);
}

#[test]
fn actionctx_local_mutations_bypass_remote_lock_and_persist_without_collection_writes() {
    let installed = addon("https://installed.example/manifest.json", 1);
    let upgraded = addon("https://installed.example/manifest.json", 2);
    let replacement = addon("https://replacement.example/manifest.json", 1);

    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![]);
    profile.addons_locked = true;
    let (runtime, rx) = new_runtime(profile, StreamsBucket::default());
    let runtime = Arc::new(RwLock::new(runtime));

    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        let installed = installed.clone();
        let upgraded = upgraded.clone();
        let replacement = replacement.clone();
        move || {
            for action in [
                ActionCtx::InstallAddonLocal(installed),
                ActionCtx::UpgradeAddonLocal(upgraded),
                ActionCtx::ReplaceAddonLocal {
                    old: addon("https://installed.example/manifest.json", 2),
                    new: replacement.clone(),
                },
                ActionCtx::UninstallAddonLocal(replacement),
            ] {
                runtime.read().unwrap().dispatch(RuntimeAction {
                    field: None,
                    action: Action::Ctx(action),
                });
            }
        }
    });

    assert!(runtime
        .read()
        .unwrap()
        .model()
        .unwrap()
        .ctx
        .profile
        .addons
        .is_empty());
    assert!(STORAGE
        .read()
        .unwrap()
        .get(PROFILE_STORAGE_KEY)
        .is_some_and(|data| serde_json::from_str::<Profile>(data)
            .unwrap()
            .addons
            .is_empty()));
    assert!(REQUESTS.read().unwrap().is_empty());
    assert_eq!(
        EVENTS
            .read()
            .unwrap()
            .iter()
            .filter(|event| {
                matches!(
                    event
                        .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                        .unwrap(),
                    RuntimeEvent::CoreEvent(
                        crate::runtime::msg::Event::AddonInstalled { .. }
                            | crate::runtime::msg::Event::AddonUpgraded { .. }
                            | crate::runtime::msg::Event::AddonUninstalled { .. }
                    )
                )
            })
            .count(),
        4
    );
    let events = EVENTS.read().unwrap();
    assert!(events.iter().any(|event| {
        matches!(
            event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap(),
            RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonInstalled {
                transport_url,
                id,
            }) if transport_url == &installed.transport_url && id == &installed.manifest.id
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap(),
            RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonUpgraded {
                transport_url,
                id,
            }) if transport_url == &upgraded.transport_url && id == &upgraded.manifest.id
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap(),
            RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonUpgraded {
                transport_url,
                id,
            }) if transport_url == &replacement.transport_url && id == &replacement.manifest.id
        )
    }));
    assert!(events.iter().any(|event| {
        matches!(
            event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap(),
            RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonUninstalled {
                transport_url,
                id,
            }) if transport_url == &replacement.transport_url && id == &replacement.manifest.id
        )
    }));
}

#[test]
fn actionctx_remote_mutations_remain_blocked_while_addons_are_locked() {
    let installed = addon("https://installed.example/manifest.json", 1);
    let updated = addon("https://installed.example/manifest.json", 2);
    let missing = addon("https://missing.example/manifest.json", 1);
    let replacement = addon("https://replacement.example/manifest.json", 1);
    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![installed.clone()]);
    profile.addons_locked = true;
    let (runtime, _rx) = new_runtime(profile, StreamsBucket::default());

    TestEnv::run(|| {
        for action in [
            ActionCtx::InstallAddon(missing),
            ActionCtx::UpgradeAddon(updated),
            ActionCtx::UninstallAddon(installed.clone()),
            ActionCtx::ReplaceAddon {
                old: installed.clone(),
                new: replacement,
            },
        ] {
            runtime.dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(action),
            });
        }
    });

    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![installed]);
    assert!(REQUESTS.read().unwrap().is_empty());
    assert!(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).is_none());
}

#[test]
fn actionctx_local_addon_projection_replaces_unprotected_addons_without_remote_write() {
    let mut protected = addon("https://protected.example/manifest.json", 1);
    protected.flags.protected = true;
    let old = addon("https://old.example/manifest.json", 1);
    let stale = addon("https://stale.example/manifest.json", 1);
    let replacement = addon("https://replacement.example/manifest.json", 1);
    let additional = addon("https://additional.example/manifest.json", 1);
    let duplicate = addon("https://replacement.example/manifest.json", 2);
    let protected_downgrade = addon("https://protected.example/manifest.json", 2);
    let mut configuration_required = addon("https://required.example/manifest.json", 1);
    configuration_required
        .manifest
        .behavior_hints
        .configuration_required = true;
    let mut projected_protected = addon("https://projected-protected.example/manifest.json", 1);
    projected_protected.flags.protected = true;

    let mut streams = StreamsBucket::default();
    for (index, addon) in [&protected, &old, &stale].into_iter().enumerate() {
        let mut item = stream_item(addon);
        item.meta_id = format!("tt{index}");
        item.video_id = format!("tt{index}:1:0");
        streams.items.insert(
            StreamsItemKey {
                meta_id: item.meta_id.clone(),
                video_id: item.video_id.clone(),
            },
            item,
        );
    }

    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![old.clone(), protected.clone(), stale.clone()]);
    profile.addons_locked = true;
    let (runtime, rx) = new_runtime(profile, streams);
    let runtime = Arc::new(RwLock::new(runtime));
    let expected_addons = vec![protected.clone(), replacement.clone(), additional.clone()];

    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        let replacement = replacement.clone();
        let additional = additional.clone();
        move || {
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![
                    replacement.clone(),
                    duplicate,
                    protected_downgrade,
                    configuration_required,
                    projected_protected,
                    additional.clone(),
                ])),
            });
        }
    });

    let model = runtime.read().unwrap();
    assert_eq!(model.model().unwrap().ctx.profile.addons, expected_addons);
    assert!(model.model().unwrap().ctx.profile.addons_locked);
    assert_eq!(model.model().unwrap().ctx.streams.items.len(), 1);
    assert!(model
        .model()
        .unwrap()
        .ctx
        .streams
        .items
        .values()
        .all(|item| item.stream_transport_url == protected.transport_url));
    drop(model);

    assert!(REQUESTS.read().unwrap().is_empty());
    assert_eq!(
        serde_json::from_str::<Profile>(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).unwrap(),)
            .unwrap()
            .addons,
        expected_addons
    );
    assert_eq!(
        serde_json::from_str::<StreamsBucket>(
            STORAGE.read().unwrap().get(STREAMS_STORAGE_KEY).unwrap(),
        )
        .unwrap()
        .items
        .len(),
        1
    );

    let events = EVENTS.read().unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event
                        .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                        .unwrap(),
                    RuntimeEvent::CoreEvent(
                        crate::runtime::msg::Event::ProfilePushedToStorage { .. }
                    )
                )
            })
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| {
                matches!(
                    event
                        .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                        .unwrap(),
                    RuntimeEvent::CoreEvent(
                        crate::runtime::msg::Event::StreamsPushedToStorage { .. }
                    )
                )
            })
            .count(),
        1
    );
    assert!(!events.iter().any(|event| matches!(
        event
            .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
            .unwrap(),
        RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonsPulledFromAPI { .. })
    )));
    let state_position = events
        .iter()
        .position(|event| {
            matches!(
                event
                    .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                    .unwrap(),
                RuntimeEvent::NewState(_, _)
            )
        })
        .unwrap();
    let receipt_position = events
        .iter()
        .position(|event| {
            matches!(
                event
                    .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                    .unwrap(),
                RuntimeEvent::CoreEvent(
                    crate::runtime::msg::Event::AddonProjectionReplacedLocal { .. }
                )
            )
        })
        .unwrap();
    let storage_position = events
        .iter()
        .position(|event| {
            matches!(
                event
                    .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                    .unwrap(),
                RuntimeEvent::CoreEvent(crate::runtime::msg::Event::ProfilePushedToStorage { .. })
            )
        })
        .unwrap();
    assert!(state_position < receipt_position);
    assert!(receipt_position < storage_position);
    let projection_events = events
        .iter()
        .filter_map(|event| {
            match event
                .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                .unwrap()
            {
                RuntimeEvent::CoreEvent(
                    crate::runtime::msg::Event::AddonProjectionReplacedLocal { transport_urls },
                ) => Some(transport_urls),
                _ => None,
            }
        })
        .collect::<Vec<_>>();
    assert_eq!(projection_events.len(), 1);
    assert_eq!(projection_events[0].len(), 4);
    for addon in [&old, &stale, &replacement, &additional] {
        assert!(projection_events[0].contains(&addon.transport_url));
    }
}

#[test]
fn actionctx_noop_local_projection_emits_only_its_distinct_receipt() {
    let mut protected = addon("https://protected.example/manifest.json", 1);
    protected.flags.protected = true;
    let installed = addon("https://installed.example/manifest.json", 1);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, rx) = new_runtime(
        authenticated_profile(vec![protected, installed.clone()]),
        StreamsBucket::default(),
    );
    let runtime = Arc::new(RwLock::new(runtime));

    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        move || {
            // The resulting projection is byte-for-byte identical, but this accepted action is still
            // an owner boundary and therefore produces an unambiguous completion receipt.
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![installed])),
            });
        }
    });

    assert!(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).is_none());
    let events = EVENTS.read().unwrap();
    assert_eq!(events.len(), 1);
    assert!(matches!(
        events[0]
            .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
            .unwrap(),
        RuntimeEvent::CoreEvent(
            crate::runtime::msg::Event::AddonProjectionReplacedLocal { transport_urls }
        ) if transport_urls.is_empty()
    ));
}

#[test]
fn actionctx_local_projection_rejects_suspended_remote_addon_pull() {
    let mut protected = addon("https://protected.example/manifest.json", 1);
    protected.flags.protected = true;
    let old = addon("https://old.example/manifest.json", 1);
    let replacement = addon("https://replacement.example/manifest.json", 1);

    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![protected.clone(), old.clone()]);
    profile.addons_locked = true;

    let (sender, receiver) = oneshot::channel::<Box<dyn Any + Send>>();
    let receiver = Arc::new(Mutex::new(Some(receiver)));
    *FETCH_HANDLER.write().unwrap() = Box::new({
        let receiver = receiver.clone();
        move |request| match request {
            Request {
                ref url,
                ref method,
                ..
            } if url == "https://api.strem.io/api/addonCollectionGet" && method == "POST" => {
                receiver
                    .lock()
                    .unwrap()
                    .take()
                    .expect("one suspended add-on pull")
                    .map_err(|_| EnvError::Other("test response sender dropped".to_owned()))
                    .boxed_env()
            }
            request => default_fetch_handler(request),
        }
    });

    let (runtime, rx) = new_runtime(profile, StreamsBucket::default());
    let runtime = Arc::new(RwLock::new(runtime));
    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        let replacement = replacement.clone();
        move || {
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::PullAddonsFromAPI),
            });
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![replacement])),
            });
            sender
                .send(Box::new(APIResult::Ok(CollectionResponse {
                    addons: vec![old],
                    last_modified: TestEnv::now(),
                })))
                .expect("send suspended add-on pull response");
        }
    });

    let (addons, addons_locked) = {
        let runtime = runtime.read().unwrap();
        let model = runtime.model().unwrap();
        (
            model.ctx.profile.addons.clone(),
            model.ctx.profile.addons_locked,
        )
    };
    assert_eq!(addons, vec![protected.clone(), replacement.clone()]);
    assert!(addons_locked);
    assert_eq!(
        serde_json::from_str::<Profile>(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).unwrap())
            .unwrap()
            .addons,
        vec![protected, replacement]
    );
    assert_eq!(
        EVENTS
            .read()
            .unwrap()
            .iter()
            .filter(|event| matches!(
                event
                    .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                    .unwrap(),
                RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonsPulledFromAPI { .. })
            ))
            .count(),
        0,
        "the stale pull result must not emit a remote collection receipt"
    );
    assert_eq!(
        EVENTS
            .read()
            .unwrap()
            .iter()
            .filter(|event| matches!(
                event
                    .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
                    .unwrap(),
                RuntimeEvent::CoreEvent(
                    crate::runtime::msg::Event::AddonProjectionReplacedLocal { .. }
                )
            ))
            .count(),
        1
    );
}

#[test]
fn actionctx_local_projection_rejects_suspended_auth_with_old_addon_collection() {
    let mut protected = addon("https://protected.example/manifest.json", 1);
    protected.flags.protected = true;
    let old = addon("https://old.example/manifest.json", 1);
    let replacement = addon("https://replacement.example/manifest.json", 1);

    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![protected.clone(), old.clone()]);
    profile.addons_locked = true;

    let (sender, receiver) = oneshot::channel::<Box<dyn Any + Send>>();
    let receiver = Arc::new(Mutex::new(Some(receiver)));
    let old_for_fetch = old.clone();
    *FETCH_HANDLER.write().unwrap() = Box::new({
        let receiver = receiver.clone();
        move |request| match request {
            Request {
                ref url,
                ref method,
                ..
            } if url == "https://api.strem.io/api/loginWithToken" && method == "POST" => receiver
                .lock()
                .unwrap()
                .take()
                .expect("one suspended authentication")
                .map_err(|_| EnvError::Other("test response sender dropped".to_owned()))
                .boxed_env(),
            Request {
                ref url,
                ref method,
                ..
            } if url == "https://api.strem.io/api/addonCollectionGet" && method == "POST" => {
                future::ok(Box::new(APIResult::Ok(CollectionResponse {
                    addons: vec![old_for_fetch.clone()],
                    last_modified: TestEnv::now(),
                })) as Box<dyn Any + Send>)
                .boxed_env()
            }
            Request {
                ref url,
                ref method,
                ..
            } if url == "https://api.strem.io/api/datastoreGet" && method == "POST" => future::ok(
                Box::new(APIResult::Ok(LibraryItemsResponse::new())) as Box<dyn Any + Send>,
            )
            .boxed_env(),
            request => default_fetch_handler(request),
        }
    });

    let (runtime, rx) = new_runtime(profile, StreamsBucket::default());
    let runtime = Arc::new(RwLock::new(runtime));
    TestEnv::run_with_runtime(rx, runtime.clone(), {
        let runtime = runtime.clone();
        let replacement = replacement.clone();
        move || {
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::Authenticate(AuthRequest::LoginWithToken {
                    token: "new-owner-token".to_owned(),
                })),
            });
            runtime.read().unwrap().dispatch(RuntimeAction {
                field: None,
                action: Action::Ctx(ActionCtx::ReplaceAddonProjectionLocal(vec![replacement])),
            });
            sender
                .send(Box::new(APIResult::Ok(AuthResponse {
                    key: AuthKey("new_owner_auth_key".to_owned()),
                    user: User {
                        id: "new_owner_user_id".into(),
                        email: "new-owner@example.com".to_owned(),
                        ..Default::default()
                    },
                })))
                .expect("send suspended authentication response");
        }
    });

    let profile = runtime.read().unwrap().model().unwrap().ctx.profile.clone();
    assert_eq!(profile.addons, vec![protected.clone(), replacement.clone()]);
    assert_eq!(profile.auth.unwrap().key, AuthKey("auth_key".to_owned()));
    let stored_profile =
        serde_json::from_str::<Profile>(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).unwrap())
            .unwrap();
    assert_eq!(
        stored_profile.addons,
        vec![protected.clone(), replacement.clone()]
    );
    assert_eq!(
        stored_profile.auth.unwrap().key,
        AuthKey("auth_key".to_owned())
    );
    assert!(!EVENTS.read().unwrap().iter().any(|event| matches!(
        event
            .downcast_ref::<RuntimeEvent<TestEnv, TestModel>>()
            .unwrap(),
        RuntimeEvent::CoreEvent(crate::runtime::msg::Event::UserAuthenticated { .. })
            | RuntimeEvent::CoreEvent(crate::runtime::msg::Event::AddonsPulledFromAPI { .. })
    )));
}

#[test]
fn actionctx_protected_addon_cannot_be_downgraded_by_local_or_remote_install() {
    let mut protected = addon("https://protected.example/manifest.json", 1);
    protected.flags.protected = true;
    let downgrade = addon("https://protected.example/manifest.json", 2);

    let _env_mutex = TestEnv::reset().unwrap();
    let mut local_profile = authenticated_profile(vec![protected.clone()]);
    local_profile.addons_locked = true;
    let (runtime, _rx) = new_runtime(local_profile, StreamsBucket::default());
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddonLocal(downgrade.clone())),
        });
    });
    assert_eq!(
        runtime.model().unwrap().ctx.profile.addons,
        vec![protected.clone()]
    );
    assert!(REQUESTS.read().unwrap().is_empty());
    assert!(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).is_none());

    drop(_env_mutex);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![protected.clone()]),
        StreamsBucket::default(),
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddon(downgrade)),
        });
    });
    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![protected]);
    assert!(REQUESTS.read().unwrap().is_empty());
    assert!(STORAGE.read().unwrap().get(PROFILE_STORAGE_KEY).is_none());
}

#[test]
fn actionctx_local_replace_guards_leave_authenticated_profile_unchanged() {
    let old = addon("https://old.example/manifest.json", 1);
    let new = addon("https://new.example/manifest.json", 2);
    let missing = addon("https://missing.example/manifest.json", 1);
    let mut required = addon("https://required.example/manifest.json", 2);
    required.manifest.behavior_hints.configuration_required = true;

    let _env_mutex = TestEnv::reset().unwrap();
    let mut protected_old = old.clone();
    protected_old.flags.protected = true;
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![protected_old.clone()]),
        StreamsBucket::default(),
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonLocal {
                old: protected_old.clone(),
                new: new.clone(),
            }),
        });
    });
    assert_eq!(
        runtime.model().unwrap().ctx.profile.addons,
        vec![protected_old]
    );
    assert!(REQUESTS.read().unwrap().is_empty());

    drop(_env_mutex);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![old.clone()]),
        StreamsBucket::default(),
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonLocal {
                old: missing,
                new: new.clone(),
            }),
        });
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::ReplaceAddonLocal {
                old: old.clone(),
                new: required,
            }),
        });
    });
    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![old]);
    assert!(REQUESTS.read().unwrap().is_empty());
}
