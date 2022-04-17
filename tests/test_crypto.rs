use kave::crypto::{decrypt, encrypt, hmac_sign, hmac_verify, new_pw_salt};

#[test]
fn test_new_pw_salt() {
    let a = new_pw_salt().expect("error generating salt");
    assert_eq!(a.len(), 128);
    let b = new_pw_salt().expect("error generating salt");
    assert_eq!(b.len(), 128);
    assert_ne!(a, b);
}

#[test]
fn test_hmac_sign() {
    let s = "wow";
    let hash = hmac_sign(s);
    assert!(hmac_verify(s, &hash))
}

#[test]
fn test_encrypt() {
    let secret = "super secret";
    let enc = encrypt(secret).expect("encryption error");
    assert_eq!(secret, decrypt(&enc).expect("decryption error"));
}
