// A Riemann client for Go, featuring concurrency, sending events and state updates, queries,
// and feature parity with the reference implementation written in Ruby.
//
// Copyright (C) 2014 by Christopher Gilbert <christopher.john.gilbert@gmail.com>
package goryman

import ()

// Event is a wrapper for Riemann events
type Event struct {
	Ttl         float32
	Time        int64
	Tags        []string
	Host        string // Defaults to os.Hostname()
	State       string
	Service     string
	Metric      interface{} // Could be Int, Float32, Float64
	Description string
	Attributes  map[string]string
}
