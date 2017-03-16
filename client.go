package client

import (
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/fluent/fluent-logger-golang/fluent"
)

const (
	// TODO: Figure out good values for these
	maxBufferedEvents = 1024
	retryDelay        = 10 * time.Millisecond

	// FluentHostPort just points to localhost on the expected billing port. The
	// billing fluentd instance *must* be run in each pod wishing to report
	// billing events.
	FluentHostPort = "localhost:24225"
)

type Client struct {
	stop   chan struct{}
	wg     sync.WaitGroup
	events chan []byte
	logger *fluent.Fluent
}

// AmountType is a msgpack encodable enum of our potential billing metrics.
type AmountType string

const (
	ContainerSeconds AmountType = "container-seconds"
)

type Amounts map[AmountType]int64

type Event struct {
	UniqueKey          string            `json:"unique_key"`
	InternalInstanceID string            `json:"internal_instance_id"`
	Timestamp          time.Time         `json:"timestamp"`
	Amounts            Amounts           `json:"amounts"`
	Metadata           map[string]string `json:"metadata"`
}

// New creates a new billing client.
func New() (*Client, error) {
	host, port, err := net.SplitHostPort(FluentHostPort)
	if err != nil {
		return nil, err
	}
	intPort, err := strconv.Atoi(port)
	if err != nil {
		return nil, err
	}
	logger, err := fluent.New(fluent.Config{
		FluentPort:   intPort,
		FluentHost:   host,
		AsyncConnect: true,
		MaxRetry:     -1,
	})
	if err != nil {
		return nil, err
	}

	c := &Client{
		stop:   make(chan struct{}),
		events: make(chan []byte, maxBufferedEvents),
		logger: logger,
	}
	c.wg.Add(1)
	go c.loop()
	return c, nil
}

// AddAmounts writes unit increments into the billing system. If the call does
// not complete (due to a crash, etc), then data may or may not have been
// written successfully.
//
// Requests with the same `uniqueKey` can be retried indefinitely until they
// succeed, and the results will be deduped.
//
// `uniqueKey` must be set, and not blank. If in doubt, generate a uuid and set
// that as the uniqueKey.
//
// `internalInstanceID`, is *not* the external instance ID (e.g. "fluffy-bunny-47"), it is the numeric internal instance ID (e.g. "1234").
//
// `timestamp` is used to determine which time bucket the usage occured in, it is included so that the result is independent of how long processing takes.
//
// `amounts` is a map with all the various amounts we wish to charge the user for.
//
// `metadata` is a general dumping ground for other metadata you may wish to include for auditability. In general, be careful about the size of data put here. Prefer including a pointer over whole data. For example, include a report id or s3 address instead of the information in the report.
func (c *Client) AddAmounts(uniqueKey, internalInstanceID string, timestamp time.Time, amounts Amounts, metadata map[string]string) error {
	if uniqueKey == "" {
		return fmt.Errorf("billing units uniqueKey cannot be blank")
	}

	// TODO: Marshal this to something more compact than json. fluent likes
	// msgpack, but that doesn't support maps with non-string keys, and
	// implementing a codec is a nightmare. Protobuf would be nice and compact
	// for storing, but then we have to do the compiled spec dance.
	e, err := json.Marshal(Event{
		UniqueKey:          uniqueKey,
		InternalInstanceID: internalInstanceID,
		Timestamp:          timestamp,
		Amounts:            amounts,
		Metadata:           metadata,
	})
	if err != nil {
		return err
	}

	select {
	case <-c.stop:
		// TODO: Should this be a panic? We just threw away billing data!
		return fmt.Errorf("Stopping, discarding event: %v", e)
	default:
	}

	select {
	case c.events <- e: // Put event in the channel unless it is full
		return nil
	default:
		// full
	}
	// TODO: Big angry log message here, cause we're throwing away billing data
	return fmt.Errorf("Reached billing event buffer limit (%d), discarding event: %v", maxBufferedEvents, e)
}

func (c *Client) loop() {
	defer c.wg.Done()
	for done := false; !done; {
		select {
		case event := <-c.events:
			c.post(event)
		case <-c.stop:
			done = true
		}
	}

	// flush remaining events
	for done := false; !done; {
		select {
		case event := <-c.events:
			c.post(event)
		default:
			done = true
		}
	}
}

func (c *Client) post(e []byte) error {
	for {
		select {
		case <-c.stop:
			return fmt.Errorf("Billing: failed to log event: %v", string(e))
		default:
			err := c.logger.Post("billing", map[string]interface{}{"data": e})
			if err == nil {
				return nil
			}
			log.Errorf("Billing: failed to log event: %v: %v, retrying in %v", string(e), err, retryDelay)
			time.Sleep(retryDelay)
		}
	}
}

func (c *Client) Close() error {
	close(c.stop)
	c.wg.Wait()
	return c.logger.Close()
}
