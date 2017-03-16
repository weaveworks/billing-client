package client

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"

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
	events chan Event
	logger *fluent.Fluent
}

type AmountType int

const (
	ContainerSeconds AmountType = 0
)

type Event struct {
	UniqueKey          string               `msg:"unique_key"`
	InternalInstanceID string               `msg:"internal_instance_id"`
	Timestamp          time.Time            `msg:"timestamp"`
	Amounts            map[AmountType]int64 `msg:"amounts"`
	Metadata           map[string]string    `msg:"metadata"`
}

// New creates a new billing client.
func New() (*Client, error) {
	host, port, err := net.SplitHostPort(fluentHostPort)
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
		events: make(chan Event, maxBufferedEvents),
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
func (c *Client) AddAmounts(uniqueKey, internalInstanceID, timestamp time.Time, amounts map[AmountType]int64, metadata map[string]string) error {
	if uniqueKey == "" {
		return fmt.Errorf("billing units uniqueKey cannot be blank")
	}

	select {
	case <-c.stop:
		// TODO: Should this be a panic? We just threw away billing data!
		return fmt.Errorf("Stopping, discarding event: %v", e)
	default:
	}

	select {
	case c.events <- Event{
		UniqueKey:          uniqueKey,
		InternalInstanceID: internalInstanceID,
		Timestamp:          timestamp,
		Amounts:            amounts,
		Metadata:           metadata,
	}: // Put event in the channel unless it is full
		return nil
	default:
		// full
	}
	// TODO: Big angry log message here, cause we're throwing away billing data
	return fmt.Errorf("Reached billing event buffer limit (%d), discarding event: %v", maxBufferedEvents, e)
}

func (c *Client) loop() {
	defer wg.Done()
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

func (c *Client) post(e Event) error {
	for {
		select {
		case <-c.stop:
			return fmt.Errorf("Billing: failed to log event: %v", e)
		default:
			err := c.logger.Post("billing", e)
			if err == nil {
				return nil
			}
			log.Errorf("Billing: failed to log event: %v: %v, retrying in %v", e, err, retryDelay)
			time.Sleep(retryDelay)
		}
	}
}

func (c *Client) Close() error {
	close(c.stop)
	c.wg.Wait()
	return c.logger.Close()
}
