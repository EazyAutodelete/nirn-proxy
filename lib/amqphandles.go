package lib

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/rabbitmq/amqp091-go"
)

type Request struct {
	Body          io.ReadCloser
	Method        string
	Header        http.Header
	ReplyTo       string
	CorrelationId string
	Message       amqp091.Delivery
	Channel       *amqp091.Channel
	URL           *url.URL
	ctx           context.Context
}

type RabbitRequest struct {
	Body    json.RawMessage   `json:"body"`
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
}

type Response struct {
	Request *Request
	Status  int
	Body    []byte
	Channel *amqp091.Channel
	header  http.Header
}

var restExchange = "rest"
var retryExchange = "restRetry"
var responseExchange = EnvGet("REST_RESPONSE_EXCHANGE", "restResponses")
var requestQueue = EnvGet("REST_REQUEST_QUEUE", "restRequestsQueue")
var retryQueue = EnvGet("REST_RETRY_QUEUE", "restRetryQueue")

func removeUrlCredentials(rawURL string) string {
	parsedURL, err := url.Parse(rawURL)
	if err != nil {
		fmt.Println("Invalid URL:", err)
		return rawURL
	}
	userInfo := parsedURL.User.Username()
	if userInfo != "" {
		userInfo += ":****@"
	}

	loggedURL := fmt.Sprintf("%s://%s%s%s", parsedURL.Scheme, userInfo, parsedURL.Host, parsedURL.RequestURI())
	loggedURL = strings.TrimRight(loggedURL, "/")

	return loggedURL
}

func ConnectRabbitMQ() (*amqp091.Connection, error) {
	var conn *amqp091.Connection
	var err error

	queueUser := EnvGet("QUEUE_USER", "eazy")
	queuePass := EnvGet("QUEUE_PASSWORD", "Wem0uLksAXV7Hs7bfeWdps0I3fia7SXwck8Pa2q829g9LHDVpyNbOHpKDLD4GuWn")
	// rawQueueHosts := EnvGet("QUEUE_HOSTS", "localhost:5672")
	queueHostStrings := []string{"rabbit-1:5672", "rabbit-2:5672", "rabbit-3:5672"}

	queueHosts := make([]string, len(queueHostStrings))
	for i, host := range queueHostStrings {
		queueHosts[i] = fmt.Sprintf("amqp://%s:%s@%s", queueUser, queuePass, host)
	}

	for {
		for _, url := range queueHosts {
			logger.Infof("Trying to connect to RabbitMQ at %s...", removeUrlCredentials(url))
			conn, err = amqp091.Dial(url)
			if err == nil {
				logger.Infof("Connected to RabbitMQ at %s", removeUrlCredentials(url))
				return conn, nil
			}
			logger.Warnf("Failed to connect to RabbitMQ at %s: %s", removeUrlCredentials(url), err)
		}

		logger.Warnf("Failed to connect to RabbitMQ, retrying in 1 second")
		time.Sleep(1 * time.Second)
	}
}

func SetupRabbitMQConnection() *amqp091.Connection {
	for {
		conn, err := ConnectRabbitMQ()
		if err != nil {
			logger.Warnf("Failed to connect to RabbitMQ cluster: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		go func(c *amqp091.Connection) {
			<-c.NotifyClose(make(chan *amqp091.Error))
			logger.Warnf("RabbitMQ connection closed. Attempting to reconnect...")
			SetupRabbitMQConnection()
		}(conn)

		return conn
	}
}

func PrepareRabbitMQChannel(conn *amqp091.Connection, prefetch int) *amqp091.Channel {
	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open a channel: %s", err)
	}

	err = ch.Qos(prefetch, 0, false)
	if err != nil {
		logger.Fatalf("Failed to set QoS: %s", err)
	}

	err = ch.ExchangeDeclare(restExchange, "direct", true, false, false, false, nil)
	if err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", restExchange, err)
	}

	err = ch.ExchangeDeclare(retryExchange, "direct", true, false, false, false, nil)
	if err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", retryExchange, err)
	}

	err = ch.ExchangeDeclare(responseExchange, "direct", true, false, false, false, nil)
	if err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", responseExchange, err)
	}

	_, err = ch.QueueDeclare(requestQueue, true, false, false, false, amqp091.Table{"x-dead-letter-exchange": retryExchange})
	if err != nil {
		logger.Fatalf("Failed to declare queue %s: %s", requestQueue, err)
	}

	_, err = ch.QueueDeclare(retryQueue, true, false, false, false, amqp091.Table{
		"x-dead-letter-exchange": restExchange,
		"x-message-ttl":          1000,
	})
	if err != nil {
		logger.Fatalf("Failed to declare queue %s: %s", retryQueue, err)
	}

	err = ch.QueueBind(requestQueue, "", restExchange, false, nil)
	if err != nil {
		logger.Fatalf("Failed to bind queue %s to exchange %s: %s", requestQueue, restExchange, err)
	}

	err = ch.QueueBind(retryQueue, "", retryExchange, false, nil)
	if err != nil {
		logger.Fatalf("Failed to bind queue %s to exchange %s: %s", retryQueue, retryExchange, err)
	}

	return ch
}

func (r *Response) Header() http.Header {
	return r.header
}

func NewRequest(channel *amqp091.Channel, rabbitMessage amqp091.Delivery) *Request {
	var rabbitRequest RabbitRequest

	err := json.Unmarshal(rabbitMessage.Body, &rabbitRequest)
	if err != nil {
		logger.Fatalf("%s", err)
	}

	headers := make(http.Header)

	for key, value := range rabbitRequest.Headers {
		headers.Add(key, value)
	}

	if headers.Get("User-Agent") == "" {
		headers.Add("User-Agent", "DiscordBot (https://github.com/EazyAutodelete/rest-proxy, v2.3.3)")
	}

	authHeader := headers.Get("Authorization")
	if authHeader != "" && !strings.HasPrefix(authHeader, "Bot ") && !strings.HasPrefix(authHeader, "Bearer ") {
		headers.Set("Authorization", "Bot "+authHeader)
	}

	if !strings.HasPrefix(rabbitRequest.Path, "/") {
		rabbitRequest.Path = "/" + rabbitRequest.Path
	}

	// if !strings.HasPrefix(rabbitRequest.Path, "/api") {
	// 	rabbitRequest.Path = "/api" + rabbitRequest.Path
	// }

	if len(rabbitRequest.Path) == 0 {
		rabbitRequest.Path = "/"
	}

	parsedUrl, err := url.Parse(rabbitRequest.Path)

	var body io.ReadCloser

	if rabbitRequest.Method == "" {
		rabbitRequest.Method = "GET"
	}

	if len(rabbitRequest.Body) > 0 && rabbitRequest.Method != "GET" {
		switch headers.Get("Content-Type") {
		case "application/json":
			var bodyObject map[string]interface{}
			if err := json.Unmarshal(rabbitRequest.Body, &bodyObject); err == nil {
				bodyBytes, _ := json.Marshal(bodyObject)
				body = io.NopCloser(bytes.NewReader(bodyBytes))

			} else {
				fmt.Println("Error parsing JSON body:", err, string(rabbitRequest.Body))
			}

		case "application/x-www-form-urlencoded":
			var bodyString string
			if err := json.Unmarshal(rabbitRequest.Body, &bodyString); err == nil {
				formData, err := url.ParseQuery(bodyString)
				if err != nil {
					fmt.Println("Error parsing form-urlencoded body:", err)
				}

				body = io.NopCloser(strings.NewReader(formData.Encode()))

			} else {
				fmt.Println("Error parsing body as form-urlencoded string:", err)

			}

		default:
			fmt.Println("Unsupported Content-Type:", headers.Get("Content-Type"))
		}
	} else {
		body = nil
	}

	r := &Request{
		Body:          body,
		Method:        rabbitRequest.Method,
		Header:        headers,
		CorrelationId: rabbitMessage.CorrelationId,
		ReplyTo:       rabbitMessage.ReplyTo,
		Message:       rabbitMessage,
		Channel:       channel,
		URL:           parsedUrl,
		ctx:           context.Background(),
	}

	return r
}

func (r *Request) Context() context.Context {
	return r.ctx
}

func (r *Request) Ack() {
	r.Message.Ack(false)
	r.Context()
}

func (r *Response) SetStatus(status int) {
	r.Status = status
}

func (r *Response) WriteBody(body []byte) {
	r.Body = body

	if r.Status == 204 && len(r.Body) > 0 {
		r.Status = 200
	}
}

func (r *Response) Send() {
	if r.Request.ReplyTo == "" || r.Request.CorrelationId == "" || len(r.Request.ReplyTo) < 2 || len(r.Request.CorrelationId) < 2 {
		r.Request.Ack()
		return
	}

	retBody := map[string]interface{}{
		"status": r.Status,
	}

	if r.Body != nil && len(r.Body) > 0 {
		retBody["body"] = string(r.Body)
	}

	bodyString, jErr := json.Marshal(retBody)
	if jErr != nil {
		logger.Errorf("Failed to marshal response: %s %s", jErr, retBody)
	}

	err := r.Channel.Publish(restExchange, r.Request.ReplyTo, false, false, amqp091.Publishing{
		ContentType:   "application/json",
		CorrelationId: r.Request.CorrelationId,
		Body:          bodyString,
	})
	if err != nil {
		logger.Errorf("Failed to publish a message: %s", err)
	} else {
		r.Request.Ack()
	}

}

func NewResponse(ch *amqp091.Channel, incoming *Request) *Response {
	return &Response{
		Request: incoming,
		Channel: ch,
		header:  make(http.Header),
	}
}
