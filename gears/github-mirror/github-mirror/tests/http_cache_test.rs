#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use std::sync::Arc;

use github_mirror::infra::github::cache::{CacheKey, CachedResponse, HttpCache};
use github_mirror::infra::github::compression::Compression;
use github_mirror::infra::storage::sea_orm_repo::SeaOrmHttpCache;
use toolkit_db::{DBProvider, DbError};
use toolkit_security::AccessScope;
use uuid::Uuid;

const URL: &str = "https://api.github.com/repos/acme/widget/issues";

fn entry() -> CachedResponse {
    CachedResponse {
        body: r#"[{"id":1,"title":"an issue"}]"#.to_owned(),
        etag: Some("W/\"abc\"".to_owned()),
        last_modified: None,
        next_page: Some("https://api.github.com/repos/acme/widget/issues?page=2".to_owned()),
    }
}

async fn store(compression: Compression) -> SeaOrmHttpCache {
    let db = common::inmem_db().await;
    SeaOrmHttpCache::new(Arc::new(DBProvider::<DbError>::new(db)), compression)
}

#[tokio::test]
async fn a_gzipped_entry_round_trips_through_the_database() {
    let cache = store(Compression::Gzip).await;
    let tenant = Uuid::new_v4();
    let key = CacheKey::compute("GET", URL, "application/json");

    assert!(
        cache
            .get(&AccessScope::for_tenant(tenant), &key)
            .await
            .unwrap()
            .is_none()
    );

    cache
        .put(&AccessScope::for_tenant(tenant), tenant, &key, URL, entry())
        .await
        .unwrap();
    let loaded = cache
        .get(&AccessScope::for_tenant(tenant), &key)
        .await
        .unwrap()
        .expect("entry");
    assert_eq!(loaded, entry(), "compression must be invisible to callers");
}

#[tokio::test]
async fn an_uncompressed_entry_round_trips_too() {
    let cache = store(Compression::None).await;
    let tenant = Uuid::new_v4();
    let key = CacheKey::compute("GET", URL, "application/json");

    cache
        .put(&AccessScope::for_tenant(tenant), tenant, &key, URL, entry())
        .await
        .unwrap();
    assert_eq!(
        cache
            .get(&AccessScope::for_tenant(tenant), &key)
            .await
            .unwrap(),
        Some(entry())
    );
}

#[tokio::test]
async fn entries_do_not_cross_tenants() {
    let cache = store(Compression::Gzip).await;
    let key = CacheKey::compute("GET", URL, "application/json");
    let owner = Uuid::new_v4();

    cache
        .put(&AccessScope::for_tenant(owner), owner, &key, URL, entry())
        .await
        .unwrap();
    assert!(
        cache
            .get(&AccessScope::for_tenant(Uuid::new_v4()), &key)
            .await
            .unwrap()
            .is_none(),
        "another tenant must not read this entry"
    );
    assert!(
        cache
            .get(&AccessScope::for_tenant(owner), &key)
            .await
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn clearing_by_prefix_drops_only_the_matching_repository() {
    let cache = store(Compression::Gzip).await;
    let tenant = Uuid::new_v4();

    let widget = CacheKey::compute("GET", URL, "application/json");
    let other_url = "https://api.github.com/repos/acme/gadget/issues";
    let gadget = CacheKey::compute("GET", other_url, "application/json");

    cache
        .put(
            &AccessScope::for_tenant(tenant),
            tenant,
            &widget,
            URL,
            entry(),
        )
        .await
        .unwrap();
    cache
        .put(
            &AccessScope::for_tenant(tenant),
            tenant,
            &gadget,
            other_url,
            entry(),
        )
        .await
        .unwrap();

    let removed = cache
        .clear(
            &AccessScope::for_tenant(tenant),
            "https://api.github.com/repos/acme/widget",
        )
        .await
        .unwrap();
    assert_eq!(removed, 1);
    assert!(
        cache
            .get(&AccessScope::for_tenant(tenant), &widget)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        cache
            .get(&AccessScope::for_tenant(tenant), &gadget)
            .await
            .unwrap()
            .is_some(),
        "the other repository's entries must survive"
    );
}

#[tokio::test]
async fn a_row_keeps_the_compression_it_was_written_with() {
    let db = common::inmem_db().await;
    let provider = Arc::new(DBProvider::<DbError>::new(db));
    let writer = SeaOrmHttpCache::new(Arc::clone(&provider), Compression::Gzip);
    let reader = SeaOrmHttpCache::new(provider, Compression::None);
    let tenant = Uuid::new_v4();
    let scope = AccessScope::for_tenant(tenant);
    let key = CacheKey::compute("GET", URL, "application/json");

    writer
        .put(&scope, tenant, &key, URL, entry())
        .await
        .unwrap();
    assert_eq!(
        reader.get(&scope, &key).await.unwrap(),
        Some(entry()),
        "the row records gzip, so a cache configured for none still decodes it"
    );
}

#[tokio::test]
async fn a_tampered_body_is_a_miss_not_an_error() {
    use github_mirror::infra::storage::entity::http_cache;
    use sea_orm::sea_query::Expr;
    use sea_orm::{ColumnTrait, EntityTrait};
    use toolkit_db::secure::SecureUpdateExt;

    let db = common::inmem_db().await;
    let cache = SeaOrmHttpCache::new(
        Arc::new(DBProvider::<DbError>::new(db.clone())),
        Compression::Gzip,
    );
    let tenant = Uuid::new_v4();
    let scope = AccessScope::for_tenant(tenant);
    let key = CacheKey::compute("GET", URL, "application/json");
    cache.put(&scope, tenant, &key, URL, entry()).await.unwrap();

    let conn = db.conn().unwrap();
    http_cache::Entity::update_many()
        .secure()
        .scope_with(&scope)
        .filter(sea_orm::Condition::all().add(http_cache::Column::CacheKey.eq(key.as_str())))
        .col_expr(http_cache::Column::ContentHash, Expr::value("0000"))
        .exec(&conn)
        .await
        .unwrap();

    assert_eq!(
        cache.get(&scope, &key).await.unwrap(),
        None,
        "a body that fails its integrity check is dropped, not surfaced"
    );
}
