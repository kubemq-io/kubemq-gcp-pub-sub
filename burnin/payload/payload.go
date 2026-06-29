// Package payload builds and verifies burn-in message bodies. Per spec S6 the
// CRC32 is computed over the RAW Pub/Sub message body bytes (the connector
// passes the data through unmodified), and the worker-id/sequence/contenthash
// are stamped into Pub/Sub message ATTRIBUTES (a string→string map), not into
// the hashed body — so no canonicalization step is needed. The native interop
// side reads the same values back from the message tags on pubsub.{topic}.
package payload

import (
	"fmt"
	"hash/crc32"
	"math/rand/v2"
	"strconv"
	"strings"
)

// Attribute names stamped onto every burn-in publish. Pub/Sub attribute keys
// are arbitrary strings (must not start with "goog"); these survive the
// connector's attribute→tag round-trip unchanged. The three reserved tags
// (_pubsub_message_id, _pubsub_publish_time, _pubsub_ordering_key) are added by
// the connector and stripped for Pub/Sub clients, so they never collide with
// these instrumentation keys.
const (
	AttrWorkerID    = "workerId"
	AttrSequence    = "sequence"
	AttrContentHash = "contentHash"
	AttrTimestampNS = "timestampNs"
	AttrOrderingSeq = "orderingSeq"
)

// Build returns an opaque body of targetSize bytes (min 1) and its CRC32 hex
// string. Pub/Sub message data is arbitrary binary, but the body is filled with
// random lowercase-hex characters — still high-entropy for corruption
// detection, and printable for logging. Integrity is verified by re-hashing the
// received body against the contentHash attribute; the connector passes the
// data through unmodified.
func Build(targetSize int) (body []byte, crcHex string) {
	if targetSize < 1 {
		targetSize = 1
	}
	body = randomHex(targetSize)
	crcHex = fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return body, crcHex
}

// VerifyCRC checks the CRC32 hex tag against the actual body bytes.
func VerifyCRC(body []byte, crcHex string) bool {
	actual := fmt.Sprintf("%08x", crc32.ChecksumIEEE(body))
	return actual == crcHex
}

// SizeDistribution represents weighted size options for message payloads.
type SizeDistribution struct {
	sizes   []int
	weights []int
	total   int
}

// ParseDistribution parses a "size:weight,size:weight" string.
func ParseDistribution(s string) (*SizeDistribution, error) {
	d := &SizeDistribution{}
	for _, p := range strings.Split(s, ",") {
		kv := strings.SplitN(strings.TrimSpace(p), ":", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid distribution entry: %q", p)
		}
		size, err := strconv.Atoi(strings.TrimSpace(kv[0]))
		if err != nil {
			return nil, fmt.Errorf("invalid size in distribution: %q", kv[0])
		}
		weight, err := strconv.Atoi(strings.TrimSpace(kv[1]))
		if err != nil {
			return nil, fmt.Errorf("invalid weight in distribution: %q", kv[1])
		}
		d.sizes = append(d.sizes, size)
		d.weights = append(d.weights, weight)
		d.total += weight
	}
	if d.total == 0 {
		return nil, fmt.Errorf("distribution total weight must be > 0")
	}
	return d, nil
}

// SelectSize returns a size sampled from the weighted distribution.
func (d *SizeDistribution) SelectSize() int {
	r := rand.IntN(d.total)
	cumulative := 0
	for i, w := range d.weights {
		cumulative += w
		if r < cumulative {
			return d.sizes[i]
		}
	}
	return d.sizes[len(d.sizes)-1]
}

const hexAlphabet = "0123456789abcdef"

// randomHex returns n random lowercase-hex bytes (printable high-entropy body).
func randomHex(n int) []byte {
	b := make([]byte, n)
	for i := 0; i < n; {
		v := rand.Uint64()
		for j := 0; j < 16 && i < n; j++ {
			b[i] = hexAlphabet[v&0xf]
			v >>= 4
			i++
		}
	}
	return b
}
