package utils

import (
	"reflect"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage/errors"
	"github.com/openyurtio/openyurt/pkg/yurthub/storage/interfaces"
)

func ValidateKey(key interfaces.Key, validKeyType interface{}) error {
	if reflect.TypeOf(key) == reflect.TypeOf(validKeyType) {
		return errors.ErrUnrecognizedKey
	}
	if key.Key() == "" {
		return errors.ErrKeyIsEmpty
	}
	return nil
}

func ValidateKV(key interfaces.Key, content []byte, valideKeyType interface{}) error {
	if err := ValidateKey(key, valideKeyType); err != nil {
		return err
	}
	if len(content) == 0 {
		return errors.ErrKeyHasNoContent
	}
	return nil
}
