pub mod honeycomb;

pub async fn get_honeycomb(
    required_access: &[&str],
) -> anyhow::Result<Option<honeycomb::HoneyComb>> {
    match honeycomb::HoneyComb::new() {
        Ok(hc) => {
            let auth = hc.list_authorizations().await?;
            if auth.has_required_access(required_access) {
                Ok(Some(hc))
            } else {
                eprintln!(
                    "honeycomb: missing required access {:?}:\n{}",
                    required_access, auth
                );
                Ok(None)
            }
        }
        Err(e) => Err(e),
    }
}
