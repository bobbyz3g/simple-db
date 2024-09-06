package simpledb

import "testing"

func TestKV_Set(t *testing.T) {
	kv := NewKV[string]()
	tests := []struct {
		key   string
		value string
	}{
		{
			key:   "key1",
			value: "value1",
		},
		{
			key:   "key2",
			value: "value2",
		},
		{
			key:   "key3",
			value: "value3",
		},
	}

	for _, test := range tests {
		kv.Set(test.key, test.value)
	}

	for _, test := range tests {
		value, ok := kv.Get(test.key)
		if !ok {
			t.Errorf("key %s not found", test.key)
		}
		if value != test.value {
			t.Errorf("key %s value %s, want %s", test.key, value, test.value)
		}
	}
}

func TestKV_Get(t *testing.T) {
	kv := NewKV[string]()
	tests := []struct {
		hasKey bool
		key    string
		value  string
	}{
		{
			hasKey: true,
			key:    "key1",
			value:  "value1",
		},
		{
			hasKey: false,
			key:    "key2",
			value:  "",
		},
	}

	for _, test := range tests {
		if test.hasKey {
			kv.Set(test.key, test.value)
		}
	}

	for _, test := range tests {
		value, ok := kv.Get(test.key)
		if ok != test.hasKey {
			t.Errorf("key %s hasKey %v, want %v", test.key, ok, test.hasKey)
		}
		if value != test.value {
			t.Errorf("key %s value %s, want %s", test.key, value, test.value)
		}
	}
}
