package utils

import (
	"reflect"

	"github.com/openyurtio/openyurt/pkg/yurthub/storage"
)

// TODO: should also valid the key format
func ValidateKey(key storage.Key, validKeyType interface{}) error {
	if reflect.TypeOf(key) != reflect.TypeOf(validKeyType) {
		return storage.ErrUnrecognizedKey
	}
	if key.Key() == "" {
		return storage.ErrKeyIsEmpty
	}
	return nil
}

func ValidateKV(key storage.Key, content []byte, valideKeyType interface{}) error {
	if err := ValidateKey(key, valideKeyType); err != nil {
		return err
	}
	if len(content) == 0 {
		return storage.ErrKeyHasNoContent
	}
	return nil
}
