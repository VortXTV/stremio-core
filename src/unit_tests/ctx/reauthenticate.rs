use crate::{
    constants::OFFICIAL_ADDONS,
    models::ctx::{Ctx, CtxStatus},
    runtime::{
        msg::{CtxAuthResponse, Internal, Msg},
        Env, EnvError, Update,
    },
    types::{
        api::AuthRequest,
        events::DismissedEventsBucket,
        library::{LibraryBucket, LibraryItem},
        notifications::NotificationsBucket,
        profile::{Auth, AuthKey, Profile, Settings, User},
        resource::MetaItemPreview,
        search_history::SearchHistoryBucket,
        server_urls::ServerUrlsBucket,
        streams::StreamsBucket,
    },
    unit_tests::TestEnv,
};
use chrono::Duration;
use serde_json::json;

fn fixture() -> (Ctx, AuthRequest, Auth) {
    let auth = Auth {
        key: AuthKey("old-fixture-key".into()),
        user: User { id: "fixture-owner".into(), ..Default::default() },
    };
    let uid = Some(auth.user.id.clone());
    let request = AuthRequest::LoginWithToken { token: "new-fixture-key".into() };
    let mut profile = Profile { auth: Some(auth.clone()), ..Default::default() };
    profile.settings.binge_watching = false;
    // Deliberately distinct from the official fallback collection.
    profile.addons.clear();
    let meta: MetaItemPreview = serde_json::from_value(json!({
        "id": "tt0000001", "name": "Local fixture", "type": "movie"
    })).unwrap();
    let mut item = LibraryItem::from((&meta, std::marker::PhantomData::<TestEnv>));
    item.removed = false;
    item.temp = false;
    item.state.time_offset = 12345;
    item.state.duration = 90000;
    let mut tombstone = item.clone();
    tombstone.id = "tt0000002".into();
    tombstone.removed = true;
    let mut search = SearchHistoryBucket::new(uid.clone());
    search.items.insert("retained local search".into(), TestEnv::now());
    let streams: StreamsBucket = serde_json::from_value(json!({
        "uid": "fixture-owner",
        "items": [[
            {"metaId":"tt0000001", "videoId":"tt0000001"},
            {
                "stream":{"url":"https://fixture.invalid/video.mp4"},
                "type":"movie", "metaId":"tt0000001", "videoId":"tt0000001",
                "metaTransportUrl":"https://fixture.invalid/manifest.json",
                "streamTransportUrl":"https://fixture.invalid/manifest.json",
                "state":{"subtitleDelay":250, "playbackSpeed":1.25},
                "_mtime":"2026-01-01T00:00:00Z"
            }
        ]]
    })).unwrap();
    let mut urls = ServerUrlsBucket::new::<TestEnv>(uid.clone());
    urls.add_url::<TestEnv>("https://fixture.invalid/streaming".parse().unwrap());
    let mut ctx = Ctx::new(
        profile, LibraryBucket::new(uid.clone(), vec![item, tombstone]), streams, urls,
        NotificationsBucket::new::<TestEnv>(uid.clone(), vec![]), search,
        DismissedEventsBucket::new(uid),
    );
    ctx.status = CtxStatus::Loading(request.clone());
    let mut refreshed_auth = auth;
    refreshed_auth.key = AuthKey("new-fixture-key".into());
    (ctx, request, refreshed_auth)
}

fn auth_result(request: AuthRequest, auth: Auth, items: Option<Vec<LibraryItem>>) -> Msg {
    Msg::Internal(Internal::CtxAuthResult(request, Ok(CtxAuthResponse {
        auth,
        addons_result: Err(EnvError::Fetch("fixture outage".into()).into()),
        library_items_result: match items {
            Some(items) => Ok(items),
            None => Err(EnvError::Fetch("fixture outage".into()).into()),
        },
    })))
}

// Drive the actual production reducer. Returned effects are intentionally not
// executed, so these tests never perform network I/O or access real storage.
#[test]
fn same_uid_library_outage_retains_progress_and_tombstones() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let expected = ctx.library.clone();
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, None));
    assert_eq!(ctx.library, expected);
}

#[test]
fn same_uid_empty_remote_library_does_not_remove_local_items() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let expected = ctx.library.clone();
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, Some(vec![])));
    assert_eq!(ctx.library, expected);
}

#[test]
fn same_uid_stale_remote_item_does_not_resurrect_local_tombstone() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let expected = ctx.library.clone();
    let mut stale = ctx.library.items["tt0000002"].clone();
    stale.removed = false;
    stale.mtime -= Duration::seconds(1);
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, Some(vec![stale])));
    assert_eq!(ctx.library, expected);
}

#[test]
fn same_uid_newer_remote_item_merges_without_dropping_other_items() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let retained = ctx.library.items["tt0000002"].clone();
    let mut newer = ctx.library.items["tt0000001"].clone();
    newer.state.time_offset = 45678;
    newer.mtime += Duration::seconds(1);
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, Some(vec![newer.clone()])));
    assert_eq!(ctx.library.items["tt0000001"], newer);
    assert_eq!(ctx.library.items["tt0000002"], retained);
}

#[test]
fn same_uid_addon_outage_preserves_preferences_and_addons_but_refreshes_auth() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let expected_settings = ctx.profile.settings.clone();
    let expected_addons = ctx.profile.addons.clone();
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth.clone(), None));
    assert_eq!(ctx.profile.auth, Some(auth));
    assert_eq!(ctx.profile.settings, expected_settings);
    assert_eq!(ctx.profile.addons, expected_addons);
    assert!(ctx.profile.addons_locked);
}

#[test]
fn same_uid_preserves_search_stream_preferences_and_server_urls() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, auth) = fixture();
    let expected_search = ctx.search_history.clone();
    let expected_streams = ctx.streams.clone();
    let expected_urls = ctx.streaming_server_urls.clone();
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, None));
    assert_eq!(ctx.search_history, expected_search);
    assert_eq!(ctx.streams, expected_streams);
    assert_eq!(ctx.streaming_server_urls, expected_urls);
}

#[test]
fn different_uid_does_not_inherit_previous_account_data_on_fetch_failure() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, request, mut auth) = fixture();
    auth.user.id = "different-fixture-owner".into();
    let uid = Some(auth.user.id.clone());
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(request, auth, None));
    assert_eq!(ctx.library, LibraryBucket::new(uid.clone(), vec![]));
    assert_eq!(ctx.profile.settings, Settings::default());
    assert_eq!(ctx.profile.addons, OFFICIAL_ADDONS.clone());
    assert_eq!(ctx.search_history, SearchHistoryBucket::new(uid.clone()));
    assert_eq!(ctx.streams, StreamsBucket::new(uid.clone()));
    assert_eq!(ctx.streaming_server_urls, ServerUrlsBucket::new::<TestEnv>(uid));
}

#[test]
fn stale_auth_result_is_ignored() {
    let _env = TestEnv::reset().unwrap();
    let (mut ctx, _, auth) = fixture();
    let before = serde_json::to_value(&ctx.profile).unwrap();
    let library = ctx.library.clone();
    let search = ctx.search_history.clone();
    let stale_request = AuthRequest::LoginWithToken { token: "stale-fixture-token".into() };
    let _effects = <Ctx as Update<TestEnv>>::update(&mut ctx, &auth_result(stale_request, auth, None));
    assert_eq!(serde_json::to_value(&ctx.profile).unwrap(), before);
    assert_eq!(ctx.library, library);
    assert_eq!(ctx.search_history, search);
}

