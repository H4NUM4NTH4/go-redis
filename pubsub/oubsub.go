package pubsub

import (
	"fmt"
	"sync"
)

// Subscriber represents one subscribed client
// Each subscriber has a channel that receives messages
// Think of it like one person's radio receiver
type Subscriber struct {
	Channel chan string // Messages arrive here
	ID      string     // Unique ID for this subscriber
}

// PubSub manages all channels and their subscribers
// Like a radio tower managing all frequencies and listeners
type PubSub struct {
	mu          sync.RWMutex
	// Map of channel name → list of subscribers
	// "news" → [sub1, sub2, sub3]
	subscribers map[string][]*Subscriber
}

// NewPubSub creates a new PubSub manager
func NewPubSub() *PubSub {
	return &PubSub{
		subscribers: make(map[string][]*Subscriber),
	}
}

// Subscribe adds a client to a channel
// Returns a Subscriber with a channel to receive messages on
// Like tuning a radio to a frequency
func (ps *PubSub) Subscribe(clientID, channelName string) *Subscriber {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Create a subscriber with a buffered channel
	// Buffer of 100 means up to 100 messages can queue up
	// before the client reads them
	sub := &Subscriber{
		Channel: make(chan string, 100),
		ID:      clientID,
	}

	// Add this subscriber to the channel's list
	ps.subscribers[channelName] = append(
		ps.subscribers[channelName],
		sub,
	)

	fmt.Printf("Client %s subscribed to '%s'\n", clientID, channelName)
	return sub
}

// Unsubscribe removes a client from a channel
// Like turning off the radio
func (ps *PubSub) Unsubscribe(clientID, channelName string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	subs, ok := ps.subscribers[channelName]
	if !ok {
		return
	}

	// Filter out this subscriber from the list
	// Like removing one person from a WhatsApp group
	newSubs := make([]*Subscriber, 0)
	for _, sub := range subs {
		if sub.ID != clientID {
			newSubs = append(newSubs, sub)
		}
	}

	ps.subscribers[channelName] = newSubs
	fmt.Printf("Client %s unsubscribed from '%s'\n", clientID, channelName)
}

// UnsubscribeAll removes a client from ALL channels
// Called when a client disconnects
func (ps *PubSub) UnsubscribeAll(clientID string) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	for channelName, subs := range ps.subscribers {
		newSubs := make([]*Subscriber, 0)
		for _, sub := range subs {
			if sub.ID != clientID {
				newSubs = append(newSubs, sub)
			}
		}
		ps.subscribers[channelName] = newSubs
	}

	fmt.Printf("Client %s unsubscribed from all channels\n", clientID)
}

// Publish sends a message to ALL subscribers of a channel
// Like a radio station broadcasting to all listeners
// Returns the number of clients that received the message
func (ps *PubSub) Publish(channelName, message string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	subs, ok := ps.subscribers[channelName]
	if !ok {
		return 0 // Nobody listening on this channel
	}

	delivered := 0
	for _, sub := range subs {
		// Non-blocking send — if subscriber's buffer is full, skip them
		// Like a radio that skips bad receivers
		select {
		case sub.Channel <- message:
			delivered++
		default:
			// Buffer full — subscriber is too slow, skip
			fmt.Printf("Warning: subscriber %s buffer full, skipping\n", sub.ID)
		}
	}

	fmt.Printf("Published to '%s': %d clients received\n", channelName, delivered)
	return delivered
}

// NumSubscribers returns how many clients are subscribed to a channel
func (ps *PubSub) NumSubscribers(channelName string) int {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	return len(ps.subscribers[channelName])
}