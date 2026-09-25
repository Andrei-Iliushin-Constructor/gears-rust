use super::graphql_url;

#[test]
fn github_com_serves_graphql_beside_its_rest_root() {
    assert_eq!(
        graphql_url("https://api.github.com"),
        "https://api.github.com/graphql"
    );
    assert_eq!(
        graphql_url("https://api.github.com/"),
        "https://api.github.com/graphql"
    );
}

#[test]
fn an_enterprise_server_serves_graphql_under_api_not_under_v3() {
    assert_eq!(
        graphql_url("https://ghe.local/api/v3"),
        "https://ghe.local/api/graphql"
    );
    assert_eq!(
        graphql_url("https://ghe.local/api/v3/"),
        "https://ghe.local/api/graphql"
    );
}

#[test]
fn any_other_base_keeps_graphql_under_it() {
    assert_eq!(
        graphql_url("http://127.0.0.1:8080"),
        "http://127.0.0.1:8080/graphql"
    );
}
