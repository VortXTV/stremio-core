mod legacy;

mod http_transport;
pub use http_transport::*;

use http::Request;

/// Identifies requests issued by [`AddonHTTPTransport`] so native environments can apply
/// add-on-specific network policy without changing unrelated streaming or LAN requests.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct AddonHTTPTransportRequest;

pub(crate) fn addon_request<T>(mut request: Request<T>) -> Request<T> {
    request.extensions_mut().insert(AddonHTTPTransportRequest);
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addon_request_is_marked_without_marking_plain_requests() {
        let plain = Request::get("https://example.com/manifest.json")
            .body(())
            .expect("request builder failed");
        assert!(plain
            .extensions()
            .get::<AddonHTTPTransportRequest>()
            .is_none());

        let marked = addon_request(plain);
        assert_eq!(
            marked.extensions().get::<AddonHTTPTransportRequest>(),
            Some(&AddonHTTPTransportRequest)
        );
    }
}
