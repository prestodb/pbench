package presto

type BiMap[K comparable, V comparable] struct {
	a map[K]V
	b map[V]K
}

func NewBiMap[K comparable, V comparable](input map[K]V) *BiMap[K, V] {
	b := make(map[V]K)
	for k, v := range input {
		b[v] = k
	}
	return &BiMap[K, V]{
		a: input,
		b: b,
	}
}

func (m *BiMap[K, V]) Lookup(key K) (V, bool) {
	value, ok := m.a[key]
	return value, ok
}

func (m *BiMap[K, V]) DirectLookup(key K) V {
	return m.a[key]
}

func (m *BiMap[K, V]) RLookup(value V) (K, bool) {
	key, ok := m.b[value]
	return key, ok
}

func (m *BiMap[K, V]) DirectRLookup(value V) K {
	return m.b[value]
}
