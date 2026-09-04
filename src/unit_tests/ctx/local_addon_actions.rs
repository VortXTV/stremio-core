use crate::constants::PROFILE_STORAGE_KEY;
use crate::models::ctx::Ctx;
use crate::runtime::msg::{Action, ActionCtx};
use crate::runtime::{Env, EnvFutureExt, Runtime, RuntimeAction, RuntimeEvent, TryEnvFuture};
use crate::types::addon::{Descriptor, Manifest};
use crate::types::api::{APIResult, SuccessResponse};
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
use futures::channel::mpsc::Receiver;
use futures::future;
use semver::Version;
use std::any::Any;
use std::sync::{Arc, RwLock};
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
fn actionctx_local_addon_guards_leave_authenticated_profile_unchanged() {
    let installed = addon("https://installed.example/manifest.json", 1);
    let missing = addon("https://missing.example/manifest.json", 1);
    let mut required = addon("https://required.example/manifest.json", 1);
    required.manifest.behavior_hints.configuration_required = true;

    let _env_mutex = TestEnv::reset().unwrap();
    let mut profile = authenticated_profile(vec![installed.clone()]);
    profile.addons_locked = true;
    let (runtime, _rx) = new_runtime(profile, StreamsBucket::default());
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddonLocal(missing.clone())),
        });
    });
    assert_eq!(
        runtime.model().unwrap().ctx.profile.addons,
        vec![installed.clone()]
    );
    assert!(REQUESTS.read().unwrap().is_empty());

    drop(_env_mutex);
    let _env_mutex = TestEnv::reset().unwrap();
    let (runtime, _rx) = new_runtime(
        authenticated_profile(vec![installed.clone()]),
        StreamsBucket::default(),
    );
    TestEnv::run(|| {
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddonLocal(installed.clone())),
        });
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::InstallAddonLocal(required)),
        });
        runtime.dispatch(RuntimeAction {
            field: None,
            action: Action::Ctx(ActionCtx::UninstallAddonLocal(missing)),
        });
    });
    assert_eq!(runtime.model().unwrap().ctx.profile.addons, vec![installed]);
    assert!(REQUESTS.read().unwrap().is_empty());
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
