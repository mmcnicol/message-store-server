# message-store-server

implements a REST API on top of [message store](https://github.com/mmcnicol/message-store) library

## usage

http request:  
POST localhost:8080/produce?topic=blaa
```
{
    "key": "MTIz", // "123" base64 encoded
    "value": "dGhpcyBpcyBhIHRlc3Q=", // "this is a test" base64 encoded
    "timestamp": "2022-03-17T15:04:05Z"
}
```

http response code: 201
http response header: "x-offset": "0" 


http request:  
GET localhost:8080/consume?topic=blaa&offset=0

http response code: 200  
http response body:
```
{
    "key": "MTIz", // "123" base64 encoded
    "value": "dGhpcyBpcyBhIHRlc3Q=", // "this is a test" base64 encoded
    "timestamp": "2022-03-17T15:04:05Z"
}
```


http request:  
GET localhost:8080/poll?topic=blaa&offset=0&pollDuration=100ms

http response code: 200  
http response body:
```
{
    "key": "MTIz", // "123" base64 encoded
    "value": "dGhpcyBpcyBhIHRlc3Q=", // "this is a test" base64 encoded
    "timestamp": "2022-03-17T15:04:05Z"
}
```

or, if no new entry exists in the topic

http response code: 204  
http response body: (empty)


## associated projects

[message store sdk](https://github.com/mmcnicol/message-store-sdk)

