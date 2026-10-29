package availability

import "time"

type Status string

const (
	AvailableRegistrarConfirmed Status = "available_registrar_confirmed"
	AvailableLikely             Status = "available_likely"
	RegisteredActive            Status = "registered_active"
	RegisteredNoDNS             Status = "registered_no_dns"
	PendingDelete               Status = "pending_delete"
	RedemptionPeriod            Status = "redemption_period"
	Reserved                    Status = "reserved"
	RestrictedTLD               Status = "restricted_tld"
	Premium                     Status = "premium"
	UnsupportedTLD              Status = "unsupported_tld"
	RateLimited                 Status = "rate_limited"
	Ambiguous                   Status = "ambiguous"
	Error                       Status = "error"
	Ignored                     Status = "ignored"
)

type Confidence string

const (
	ConfidenceLow     Confidence = "low"
	ConfidenceMedium  Confidence = "medium"
	ConfidenceHigh    Confidence = "high"
	ConfidenceHighest Confidence = "highest"
)

type Signals struct {
	DNSPresent         bool
	RDAPFound          bool
	RDAPNotFound       bool
	RDAPPendingDelete  bool
	RDAPRedemption     bool
	RDAPReserved       bool
	RegistrarAvailable bool
	RegistrarPremium   bool
	RestrictedTLD      bool
	UnsupportedTLD     bool
	ReservedTLD        bool
	RateLimited        bool
	Error              bool
}

type Classification struct {
	Status     Status
	Confidence Confidence
}

func Classify(signals Signals) Classification {
	switch {
	case signals.RateLimited:
		return Classification{Status: RateLimited, Confidence: ConfidenceLow}
	case signals.RestrictedTLD:
		return Classification{Status: RestrictedTLD, Confidence: ConfidenceHigh}
	case signals.UnsupportedTLD:
		return Classification{Status: UnsupportedTLD, Confidence: ConfidenceMedium}
	case signals.ReservedTLD:
		return Classification{Status: Reserved, Confidence: ConfidenceHigh}
	case signals.RDAPReserved:
		return Classification{Status: Reserved, Confidence: ConfidenceHigh}
	case signals.RDAPPendingDelete:
		return Classification{Status: PendingDelete, Confidence: ConfidenceHigh}
	case signals.RDAPRedemption:
		return Classification{Status: RedemptionPeriod, Confidence: ConfidenceHigh}
	case signals.RegistrarPremium:
		return Classification{Status: Premium, Confidence: ConfidenceHigh}
	case signals.RegistrarAvailable:
		return Classification{Status: AvailableRegistrarConfirmed, Confidence: ConfidenceHighest}
	case signals.RDAPFound && signals.DNSPresent:
		return Classification{Status: RegisteredActive, Confidence: ConfidenceHigh}
	case signals.RDAPFound:
		return Classification{Status: RegisteredNoDNS, Confidence: ConfidenceMedium}
	case signals.RDAPNotFound && !signals.DNSPresent:
		return Classification{Status: AvailableLikely, Confidence: ConfidenceMedium}
	case signals.Error:
		return Classification{Status: Error, Confidence: ConfidenceLow}
	default:
		return Classification{Status: Ambiguous, Confidence: ConfidenceLow}
	}
}

type TTLPolicy struct {
	Registered     time.Duration
	Available      time.Duration
	Likely         time.Duration
	Ambiguous      time.Duration
	UnsupportedTLD time.Duration
	RateLimited    time.Duration
	Error          time.Duration
}

func DefaultTTLPolicy() TTLPolicy {
	return TTLPolicy{
		Registered:     14 * 24 * time.Hour,
		Available:      time.Hour,
		Likely:         6 * time.Hour,
		Ambiguous:      12 * time.Hour,
		UnsupportedTLD: 60 * 24 * time.Hour,
		RateLimited:    time.Hour,
		Error:          30 * time.Minute,
	}
}

func (p TTLPolicy) TTL(status Status) time.Duration {
	switch status {
	case RegisteredActive, RegisteredNoDNS, PendingDelete, RedemptionPeriod, Reserved, Premium:
		return p.Registered
	case AvailableRegistrarConfirmed:
		return p.Available
	case AvailableLikely:
		return p.Likely
	case UnsupportedTLD, RestrictedTLD:
		return p.UnsupportedTLD
	case RateLimited:
		return p.RateLimited
	case Error:
		return p.Error
	default:
		return p.Ambiguous
	}
}
