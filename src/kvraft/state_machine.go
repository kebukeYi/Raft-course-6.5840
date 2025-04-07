package kvraft

type MemoryKVStateMachine struct {
	KV map[string]string
}

func NewMemoryKVStateMachine() *MemoryKVStateMachine {
	return &MemoryKVStateMachine{
		KV: make(map[string]string),
	}
}

func (m *MemoryKVStateMachine) Get(key string) (string, Err) {
	if value, ok := m.KV[key]; ok {
		return value, OK
	}
	return "", ErrNoKey
}

func (m *MemoryKVStateMachine) Put(key, value string) Err {
	m.KV[key] = value
	return OK
}

func (m *MemoryKVStateMachine) Append(key, value string) Err {
	m.KV[key] += value
	return OK
}
