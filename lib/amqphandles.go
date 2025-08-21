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
	header  http.Header
}

var restExchange = "rest"
var retryExchange = "restRetry"
var responseExchange = EnvGet("REST_RESPONSE_EXCHANGE", "restResponses")
var requestQueue = EnvGet("REST_REQUEST_QUEUE", "restRequestsQueue")
var retryQueue = EnvGet("REST_RETRY_QUEUE", "restRetryQueue")

/*
Optionale Getter (praktisch für Aufrufer)
*/
func GetRequestQueue() string  { return requestQueue }
func RestExchange() string     { return restExchange }
func RetryExchange() string    { return retryExchange }
func ResponseExchange() string { return responseExchange }

/*
Rabbit – zentrale Struktur
- Verbindet sich automatisch
- Deklariert Exchanges/Queues
- Startet registrierte Consumer nach jedem (Re)Connect neu
- Verwendet pro Publish einen eigenen Channel (thread-safe)
- Verwendet pro Consumer einen eigenen Channel (channels sind NICHT goroutine-safe)
*/
type Rabbit struct {
	conn     *amqp091.Connection
	adminCh  *amqp091.Channel // Topology/Declares
	prefetch int
	handlers []func(*amqp091.Channel) // Consumer-Fabriken: bekommen jeweils einen frischen Channel
}

var rabbit *Rabbit

// SetupRabbitMQ initialisiert die globale Rabbit-Struktur und verbindet.
func SetupRabbitMQ(prefetch int) {
	rabbit = &Rabbit{prefetch: prefetch}
	rabbit.connect()
}

// GetRabbit liefert die globale Rabbit-Instanz.
func GetRabbit() *Rabbit { return rabbit }

// RegisterConsumer registriert einen Consumer-Handler.
// Er wird sofort gestartet (falls verbunden) und automatisch nach Reconnect neu gestartet.
func (r *Rabbit) RegisterConsumer(handler func(*amqp091.Channel)) {
	r.handlers = append(r.handlers, handler)
	if r.conn != nil && !r.conn.IsClosed() {
		r.startHandler(handler)
	}
}

/*
Verbindung herstellen + Topologie deklarieren + Consumer starten
*/
func (r *Rabbit) connect() {
	for {
		conn, err := ConnectRabbitMQ()
		if err != nil {
			logger.Warn("failed to connect to RabbitMQ, retrying...")
			time.Sleep(time.Second)
			continue
		}

		adminCh := PrepareRabbitMQChannel(conn) // deklariert Exchanges/Queues/Binds (idempotent)

		r.conn = conn
		r.adminCh = adminCh

		logger.Infof("RabbitMQ connected; topology ready")

		// Auf Connection-Close lauschen und reconnecten
		go func() {
			<-conn.NotifyClose(make(chan *amqp091.Error))
			logger.Warn("RabbitMQ connection closed. Reconnecting...")
			r.connect()
		}()

		// Alle registrierten Consumer starten (jeweils eigener Channel mit QoS)
		for _, h := range r.handlers {
			r.startHandler(h)
		}

		return
	}
}

// startHandler öffnet einen frischen Channel mit QoS und startet den Consumer-Handler darauf.
func (r *Rabbit) startHandler(h func(*amqp091.Channel)) {
	ch, err := r.conn.Channel()
	if err != nil {
		logger.Errorf("failed to open consumer channel: %v", err)
		return
	}
	if r.prefetch > 0 {
		if err := ch.Qos(r.prefetch, 0, false); err != nil {
			logger.Errorf("failed to set QoS on consumer channel: %v", err)
			_ = ch.Close()
			return
		}
	}
	go func() {
		// Handler blockiert typischerweise mit ch.Consume(..); wenn die Connection stirbt, endet es.
		h(ch)
		// Nach Beendigung Channel schließen (falls noch offen)
		_ = ch.Close()
	}()
}

/*
Publishing

WICHTIG: amqp091.Channel ist NICHT goroutine-safe. Daher wird für jeden Publish ein eigener Channel geöffnet.
Damit sind parallele Publishes sicher, ohne globale Locks.
*/
func (r *Rabbit) PublishBytes(exchange, key string, body []byte, correlationId, replyTo string) error {
	if r.conn == nil || r.conn.IsClosed() {
		return fmt.Errorf("connection is not open")
	}
	ch, err := r.conn.Channel()
	if err != nil {
		return err
	}
	defer ch.Close()

	return ch.Publish(exchange, key, false, false, amqp091.Publishing{
		ContentType:   "application/json",
		CorrelationId: correlationId,
		ReplyTo:       replyTo,
		Body:          body,
	})
}

func (r *Rabbit) PublishJSON(exchange, key string, v interface{}, correlationId, replyTo string) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return r.PublishBytes(exchange, key, b, correlationId, replyTo)
}

/*
	Low-level Connection + Topologie
*/

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

func PrepareRabbitMQChannel(conn *amqp091.Connection) *amqp091.Channel {
	ch, err := conn.Channel()
	if err != nil {
		logger.Fatalf("Failed to open a channel: %s", err)
	}

	// Topologie deklarieren (idempotent)
	if err := ch.ExchangeDeclare(restExchange, "direct", true, false, false, false, nil); err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", restExchange, err)
	}
	if err := ch.ExchangeDeclare(retryExchange, "direct", true, false, false, false, nil); err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", retryExchange, err)
	}
	if err := ch.ExchangeDeclare(responseExchange, "direct", true, false, false, false, nil); err != nil {
		logger.Fatalf("Failed to declare exchange %s: %s", responseExchange, err)
	}

	if _, err := ch.QueueDeclare(
		requestQueue, true, false, false, false,
		amqp091.Table{"x-dead-letter-exchange": retryExchange},
	); err != nil {
		logger.Fatalf("Failed to declare queue %s: %s", requestQueue, err)
	}

	if _, err := ch.QueueDeclare(
		retryQueue, true, false, false, false,
		amqp091.Table{
			"x-dead-letter-exchange": restExchange,
			"x-message-ttl":          int32(1000),
		},
	); err != nil {
		logger.Fatalf("Failed to declare queue %s: %s", retryQueue, err)
	}

	if err := ch.QueueBind(requestQueue, "", restExchange, false, nil); err != nil {
		logger.Fatalf("Failed to bind queue %s to exchange %s: %s", requestQueue, restExchange, err)
	}
	if err := ch.QueueBind(retryQueue, "", retryExchange, false, nil); err != nil {
		logger.Fatalf("Failed to bind queue %s to exchange %s: %s", retryQueue, retryExchange, err)
	}

	return ch
}

func (r *Response) Header() http.Header {
	if r.header == nil {
		r.header = make(http.Header)
	}
	return r.header
}

func NewRequest(rabbitMessage amqp091.Delivery) *Request {
	var rabbitRequest RabbitRequest
	if err := json.Unmarshal(rabbitMessage.Body, &rabbitRequest); err != nil {
		logger.Fatalf("invalid request payload: %v", err)
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
	if len(rabbitRequest.Path) == 0 {
		rabbitRequest.Path = "/"
	}
	parsedUrl, err := url.Parse(rabbitRequest.Path)
	if err != nil {
		logger.Fatalf("Failed to parse request path: %v", err)
	}

	if rabbitRequest.Method == "" {
		rabbitRequest.Method = "GET"
	}

	var body io.ReadCloser
	if len(rabbitRequest.Body) > 0 && strings.ToUpper(rabbitRequest.Method) != "GET" {
		switch headers.Get("Content-Type") {
		case "application/json":
			var bodyObject map[string]interface{}
			if err := json.Unmarshal(rabbitRequest.Body, &bodyObject); err == nil {
				bodyBytes, _ := json.Marshal(bodyObject)
				body = io.NopCloser(bytes.NewReader(bodyBytes))

			} else {
				logger.Errorf("Error parsing JSON body: %v; raw=%s", err, string(rabbitRequest.Body))
			}

		case "application/x-www-form-urlencoded":
			var bodyString string
			if err := json.Unmarshal(rabbitRequest.Body, &bodyString); err == nil {
				formData, err := url.ParseQuery(bodyString)
				if err != nil {
					logger.Errorf("Error parsing form-urlencoded body: %v", err)
				}

				body = io.NopCloser(strings.NewReader(formData.Encode()))

			} else {
				logger.Errorf("Error parsing body as form-urlencoded string: %v", err)

			}

		default:
			if headers.Get("Content-Type") != "" {
				logger.Warnf("Unsupported Content-Type: %s", headers.Get("Content-Type"))
			}
		}
	}

	return &Request{
		Body:          body,
		Method:        rabbitRequest.Method,
		Header:        headers,
		CorrelationId: rabbitMessage.CorrelationId,
		ReplyTo:       rabbitMessage.ReplyTo,
		Message:       rabbitMessage,
		URL:           parsedUrl,
		ctx:           context.Background(),
	}
}

func (r *Request) Context() context.Context {
	return r.ctx
}

func (r *Request) Ack() {
	_ = r.Message.Ack(false)
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
	if r.Request.ReplyTo == "" || r.Request.CorrelationId == "" {
		r.Request.Ack()
		return
	}

	retBody := map[string]interface{}{
		"status": r.Status,
	}

	if len(r.Body) > 0 {
		retBody["body"] = string(r.Body)
	}

	payload, jErr := json.Marshal(retBody)
	if jErr != nil {
		logger.Errorf("Failed to marshal response: %v; value=%v", jErr, retBody)
		// Versuche trotzdem zu antworten mit "status" only
		_ = rabbit.PublishJSON(restExchange, r.Request.ReplyTo, map[string]interface{}{"status": r.Status}, r.Request.CorrelationId, "")
		r.Request.Ack()
		return
	}

	if err := rabbit.PublishBytes(restExchange, r.Request.ReplyTo, payload, r.Request.CorrelationId, ""); err != nil {
		logger.Errorf("Failed to publish response: %v", err)
	} else {
		r.Request.Ack()
	}
}

func NewResponse(incoming *Request) *Response {
	return &Response{
		Request: incoming,
		header:  make(http.Header),
	}
}
