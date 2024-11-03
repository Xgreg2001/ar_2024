use zad_1::client::Client;

fn main() -> Result<(), std::io::Error> {
    let client = Client::new("127.0.0.1:8888")?;
    let mut file = client.open("test.txt", "w")?;
    file.write(b"Hello, World!")?;
    Ok(())
}
