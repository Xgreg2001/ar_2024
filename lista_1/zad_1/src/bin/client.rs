use zad_1::client::Client;

fn main() -> Result<(), std::io::Error> {
    let client = Client::new("127.0.0.1:8888")?;

    // **Test `open` and `write`**
    println!("Opening file 'test.txt' for writing...");
    let file = client.open("test.txt", "w")?;
    let content = b"Hello, World!";
    println!(
        "Writing to 'test.txt': {}",
        String::from_utf8_lossy(content)
    );
    file.write(content)?;

    // **Test `open` and `read`**
    println!("Opening file 'test.txt' for reading...");
    let file = client.open("test.txt", "r")?;
    let mut buffer = vec![0u8; content.len()];
    file.read(&mut buffer)?;
    println!("Read from 'test.txt': {}", String::from_utf8_lossy(&buffer));

    // **Test `lseek`**
    println!("Seeking to the beginning of 'test.txt'...");
    file.lseek(0, 0)?; // Seek to start (whence=0)

    println!("Reading after seek...");
    let mut buffer = vec![0u8; content.len()];
    file.read(&mut buffer)?;
    println!("Read after seek: {}", String::from_utf8_lossy(&buffer));

    // **Test `chmod`**
    println!("Changing permissions of 'test.txt' to 0o644...");
    client.chmod("test.txt", 0o644)?;
    println!("Permissions changed.");

    // **Test `rename`**
    println!("Renaming 'test.txt' to 'renamed_test.txt'...");
    client.rename("test.txt", "renamed_test.txt")?;
    println!("File renamed.");

    // **Test `unlink`**
    println!("Deleting 'renamed_test.txt'...");
    client.unlink("renamed_test.txt")?;
    println!("File deleted.");

    // Attempt to open the deleted file to confirm deletion
    println!("Attempting to open deleted file 'renamed_test.txt'...");
    match client.open("renamed_test.txt", "r") {
        Ok(_) => println!("Error: Deleted file was opened."),
        Err(e) => println!("Confirmed deletion. Error opening file: {}", e),
    }

    Ok(())
}
