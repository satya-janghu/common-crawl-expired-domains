# Domain Availability Pipeline

## Goal

Determine whether a candidate registrable domain is available, likely available, registered, pending-delete, premium, reserved, restricted, unsupported, ambiguous, or errored.

## Pipeline

```text
Candidate domain
  ↓
Syntax + PSL validation
  ↓
TLD policy lookup
  ↓
Global cache lookup
  ↓
DNS prefilter
  ↓
RDAP lookup
  ↓
Registrar provider check
  ↓
Signal classifier
  ↓
Cache write + candidate update
```

## Domain validation

Reject:

- Empty domains
- IP addresses
- Domains with invalid characters
- Domains below unsupported/blocked TLDs
- Public suffixes with no registrable label
- Domains matching ignore rules

Do not reject PSL-valid domains only because their TLD is not in a curated allow-list. The default product behavior is broad coverage. Policy should classify domains under non-retail or eligibility-restricted suffixes, not silently discard them.

## TLD policy

Classify every discovered suffix into one of these categories:

```text
open_retail
restricted
reserved_infrastructure
unsupported_unknown
```

Rules:

- `open_retail`: attempt DNS, RDAP, and registrar checks where supported.
- `restricted`: mark as `restricted_tld` unless an explicit provider integration supports eligibility-aware checking.
- `reserved_infrastructure`: mark as `reserved` or `restricted_tld` depending on suffix policy.
- `unsupported_unknown`: mark as `unsupported_tld` when no RDAP/registrar path is available.
- User blocked TLDs/domains/suffixes remain `ignored`.

Initial restricted/non-retail examples:

```text
.gov
.mil
.edu
.int
.arpa
```

## DNS checks

Record types:

- NS
- A
- AAAA
- MX
- SOA

Interpretation:

- DNS records present usually imply registered/active.
- No DNS records may imply unconfigured registered domain, expired domain, or resolver issue.
- DNS absence must not be treated as definitive availability.

## RDAP checks

Use IANA RDAP bootstrap data per TLD.

RDAP statuses to handle:

- Success with entity/events/status fields
- Not found
- Rate limited
- Redirect/alternate endpoint
- Unsupported TLD
- Malformed response
- Timeout

Interpretation:

- RDAP success usually means registered.
- RDAP not found can mean likely available, but TLD-specific policy matters.
- RDAP rate limits should trigger backoff and retry.
- Registrar confirmation is gated by the pre-registrar classification. The default production profile only sends `available_likely` candidates to Porkbun; registered, restricted, reserved, unsupported, pending-delete, redemption, error, and ambiguous candidates are classified from policy/DNS/RDAP without spending registrar quota.

## Registrar checks

Registrar availability checks are the strongest signal for buyability.

Requirements:

- Pluggable provider interface.
- Porkbun is the first registrar provider.
- Provider credentials supplied through environment variables such as `PORKBUN_API_KEY` and `PORKBUN_SECRET_KEY`; config may reference env var names but must not store secret values.
- Provider-specific rate limits.
- Persist provider retry windows and defer availability tasks instead of consuming retry attempts or writing final `rate_limited` candidate rows for registrar throttling.
- Return status, price, currency, premium flag, and raw response hash.

Provider interface:

```go
type RegistrarProvider interface {
	Name() string
	RateLimitPolicy() RateLimitPolicy
	CheckAvailability(ctx context.Context, domain string) (RegistrarResult, error)
}

type RegistrarResult struct {
	Domain          string
	Status          string
	Available       bool
	Premium         bool
	PriceCents      int
	Currency        string
	RawResponseHash string
	CheckedAt       time.Time
}
```

## Classification rules

| Signals | Result |
|---|---|
| Registrar says available | `available_registrar_confirmed`, highest confidence |
| RDAP not found + DNS absent + no registrar configured | `available_likely`, medium/high depending TLD |
| RDAP success + DNS present | `registered_active` |
| RDAP success + DNS absent | `registered_no_dns` |
| RDAP status includes pending delete | `pending_delete` |
| RDAP status includes redemption | `redemption_period` |
| Registrar says premium | `premium` |
| TLD policy is restricted/non-retail | `restricted_tld` |
| TLD unsupported | `unsupported_tld` |
| Conflicting signals | `ambiguous` |
| Provider timeout/rate-limited | `rate_limited` or retry |

## Cache TTLs

| Status | Default TTL |
|---|---:|
| `available_registrar_confirmed` | 1 hour |
| `available_likely` | 6 hours |
| `registered_active`, `registered_no_dns` | 14 days |
| `pending_delete`, `redemption_period` | 14 days |
| `reserved`, `premium` | 14 days |
| `restricted_tld`, `unsupported_tld` | 60 days |
| `ambiguous` | 12 hours |
| `rate_limited` | 1 hour |
| `error` | 30 minutes |

TTL must be configurable.

Registered, available, likely-available, ambiguous and unsupported TTLs can be overridden through the `cache` section of `config.json`.

## Watchlist

Candidate domains can be added to a watchlist.

Watchlist fields:

- domain
- reason
- current status
- next_check_at
- notes
- source run ID

Watchlist domains should be recheckable manually.
