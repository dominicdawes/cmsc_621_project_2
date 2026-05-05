project2/
  main.go          # dispatcher + consolidator goroutines as gRPC servers
  worker/
    main.go        # standalone worker binary
  fileserver/
    main.go        # standalone fileserver binary
  primes/
    primes.proto   # your service definitions
    primes.pb.go   # generated (don't write this)
    primes_grpc.pb.go  # generated
  primes_config.txt
  go.mod