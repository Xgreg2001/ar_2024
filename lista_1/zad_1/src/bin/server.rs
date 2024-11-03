use zad_1::server::Server;

fn main() -> Result<(), std::io::Error> {
    let mut server = Server::new("0.0.0.0:8888")?;
    server.run()
}
