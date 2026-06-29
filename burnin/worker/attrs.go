package worker

import (
	"strconv"
	"time"

	"github.com/kubemq-io/kubemq-gcp-pub-sub/burnin/payload"
)

// stampAttrs builds the instrumentation Pub/Sub attributes carried on every
// burn-in Publish (spec S6): workerId, sequence, contentHash, timestampNs.
// Pub/Sub attribute keys/values are arbitrary strings (must not start with
// "goog") and survive the connector's attribute→tag round-trip unchanged, so a
// native consumer reads the SAME keys back as plain tags on pubsub.{topic}.
func stampAttrs(producerID string, seq uint64, crcHex string) map[string]string {
	return map[string]string{
		payload.AttrWorkerID:    producerID,
		payload.AttrSequence:    strconv.FormatUint(seq, 10),
		payload.AttrContentHash: crcHex,
		payload.AttrTimestampNS: strconv.FormatInt(time.Now().UnixNano(), 10),
	}
}

// extractMeta pulls (producerID, seq, crcHex, sentAt) from a Pub/Sub message's
// attribute map. The native interop side reads the identical keys from the
// Events Store message tags (the connector maps attributes→tags 1:1). Missing or
// garbled fields yield zero values with ok=false.
func extractMeta(attrs map[string]string) (producerID string, seq uint64, crcHex string, sentAt time.Time, ok bool) {
	if attrs == nil {
		return "", 0, "", time.Time{}, false
	}
	producerID = attrs[payload.AttrWorkerID]
	crcHex = attrs[payload.AttrContentHash]
	if v := attrs[payload.AttrSequence]; v != "" {
		if n, err := strconv.ParseUint(v, 10, 64); err == nil {
			seq = n
		}
	}
	if v := attrs[payload.AttrTimestampNS]; v != "" {
		if ns, err := strconv.ParseInt(v, 10, 64); err == nil {
			sentAt = time.Unix(0, ns)
		}
	}
	ok = producerID != ""
	return producerID, seq, crcHex, sentAt, ok
}
