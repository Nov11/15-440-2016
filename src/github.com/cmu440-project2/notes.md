1.even if channel buffers are made very large(like 5000), this design works bad when client number grows.
for 1 server connected with 1 client, it's ok to process 5000 packets in 2s.
for 1 server with 2 clients, it's ok to process 2500 packets each in 2s.
but for 5 clients, a little above 30 packets each client.

2.whenever using go routine in for loop, watch out that loop variable is caught by reference by default if it is used in go routine.
most time a function parameter is needed.

3.splice prepend :
c.receiveMessageQueue = append(c.receiveMessageQueue[:i], append([]*Message{msg}, c.receiveMessageQueue[i:]...)...)
this works because the second append will create a new underline storage
splice is not the same as array in c++

4.waitgroup for count down latch