even if channel buffers are made very large(like 5000), this design works bad when client number grows.
for 1 server connected with 1 client, it's ok to process 5000 packets in 2s.
for 1 server with 2 clients, it's ok to process 2500 packets each in 2s.
but for 5 clients, a little above 30 packets each client.
