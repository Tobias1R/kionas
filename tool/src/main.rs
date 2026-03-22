use rcgen::{
    date_time_ymd, Certificate, CertificateParams, DistinguishedName, DnType, KeyPair, SanType,
};
use std::{fs, vec};

fn generate_cert(name: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut params: CertificateParams = Default::default();
    params.not_before = date_time_ymd(1975, 1, 1);
    params.not_after = date_time_ymd(4096, 1, 1);
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Kionas");
    params
        .distinguished_name
        .push(DnType::CommonName, "Master Cert");
    params.subject_alt_names = vec![
        SanType::DnsName("127.0.0.1".try_into()?),
        SanType::DnsName("localhost".try_into()?),
    ];
    params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;

    let pem_serialized = cert.pem();
    let pem = pem::parse(&pem_serialized)?;
    let der_serialized = pem.contents;
    println!("{pem_serialized}");
    println!("{}", key_pair.serialize_pem());
    fs::create_dir_all("certs/")?;
    fs::write(format!("certs/{name}.pem"), pem_serialized.as_bytes())?;
    fs::write(format!("certs/{name}.der"), der_serialized)?;
    fs::write(
        format!("certs/{name}_key.pem"),
        key_pair.serialize_pem().as_bytes(),
    )?;
    fs::write(format!("certs/{name}_key.der"), key_pair.serialize_der())?;

    let certificates = vec!["interops", "warehouse", "worker1"];
    for namex in certificates {
        issue_cert(namex, &cert, &key_pair)?;
    }

    Ok(())
}

fn issue_cert(
    name: &str,
    issuer_pem: &Certificate,
    issuer_key: &KeyPair,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut params: CertificateParams = Default::default();
    params.not_before = date_time_ymd(1975, 1, 1);
    params.not_after = date_time_ymd(4096, 1, 1);
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::OrganizationName, "Kionas");
    params
        .distinguished_name
        .push(DnType::CommonName, "Other Cert");
    params.subject_alt_names = vec![
        SanType::DnsName("127.0.0.1".try_into()?),
        SanType::DnsName("localhost".try_into()?),
    ];

    let key_pair = KeyPair::generate()?;
    let cert = params.signed_by(&key_pair, issuer_pem, issuer_key)?;

    let pem_serialized = cert.pem();
    let pem = pem::parse(&pem_serialized)?;
    let der_serialized = pem.contents;
    println!("{pem_serialized}");
    println!("{}", key_pair.serialize_pem());
    fs::create_dir_all("certs/")?;
    fs::write(format!("certs/{name}.pem"), pem_serialized.as_bytes())?;
    fs::write(format!("certs/{name}.der"), der_serialized)?;
    fs::write(
        format!("certs/{name}_key.pem"),
        key_pair.serialize_pem().as_bytes(),
    )?;
    fs::write(format!("certs/{name}_key.der"), key_pair.serialize_der())?;

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // remove all pem, der files
    fs::remove_dir_all("certs/")?;
    fs::create_dir_all("certs/")?;
    generate_cert("CA")?;
    Ok(())
}
