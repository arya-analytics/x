package signal

import "github.com/cockroachdb/errors"

var (
	ImproperShutdown = errors.New("[signal]  - improper shutdown")
)
