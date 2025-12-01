fn main() {
    let file_descriptors = protox::compile(["shredstream.proto"], ["."]).unwrap();

    tonic_prost_build::configure()
        .build_server(false) // We only need the client
        .build_client(true)
        .out_dir("src")
        .compile_fds(file_descriptors)
        .unwrap();
}
