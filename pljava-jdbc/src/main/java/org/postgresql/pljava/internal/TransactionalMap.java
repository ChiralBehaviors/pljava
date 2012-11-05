/*
 * Copyright (c) 2004, 2005, 2006 TADA AB - Taby Sweden
 * Distributed under the terms shown in the file COPYRIGHT
 * found in the root folder of this project or at
 * http://eng.tada.se/osprojects/COPYRIGHT.html
 */
package org.postgresql.pljava.internal;

import java.util.AbstractCollection;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/**
 * A TransactionalMap acts as a modifiable front for a backing map. All
 * modifications can be reverted by a call to abort or propagated to the backing
 * map by a call to commit.
 * 
 * The map is not synchronized so care should be taken if multiple threads will
 * access the map.
 * 
 * @author Thomas Hallgren
 */
public class TransactionalMap extends HashMap<Object, Object> {
    protected class BackedEntry implements Map.Entry<Object, Object> {
        private Object m_key;

        public BackedEntry(Object key) {
            m_key = key;
        }

        public Object getKey() {
            return m_key;
        }

        public Object getValue() {
            return get(m_key);
        }

        public Object setValue(Object value) {
            return put(m_key, value);
        }
    }

    protected class EntryIterator extends KeyIterator {
        @Override
        public Object next() {
            return new BackedEntry(super.next());
        }
    }

    protected class KeyIterator implements Iterator<Object> {
        private Iterator<Object> m_currentItor = superKeySet().iterator();

        private Object           m_currentKey  = null;

        private boolean          m_phaseA      = true;

        public boolean hasNext() {
            m_currentKey = getValidKey(m_currentKey);
            return m_currentKey != null;
        }

        public Object next() {
            Object key = getValidKey(m_currentKey);
            if (key == null) {
                throw new NoSuchElementException();
            }

            m_currentKey = null; // Force retrieval of next key
            return key;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        protected Object getValidKey(Object key) {
            if (key != null && containsKey(key)) {
                return key;
            }

            // Entry is not valid. Get next entry.
            //
            for (;;) {
                while (m_currentItor.hasNext()) {
                    key = m_currentItor.next();
                    if (containsKey(key)) {
                        return key;
                    }
                }

                if (!m_phaseA) {
                    break;
                }

                m_currentItor = m_base.keySet().iterator();
                m_phaseA = false;
            }
            return null;
        }
    }

    protected class ValueIterator extends KeyIterator {
        @Override
        public Object next() {
            return get(super.next());
        }
    }

    // The object representing no object (i.e. a shadowed entry)
    //
    private static final Object       s_noObject       = new Object();

    private static final long         serialVersionUID = 5337569423915578121L;

    // Commited data
    //
    private final Map<Object, Object> m_base;

    // Cache of backed collections.
    //
    private Set<?>                    m_entrySet;

    private Set<?>                    m_keySet;

    private Collection<?>             m_valueColl;

    protected TransactionalMap(Map<Object, Object> base) {
        m_base = base;
    }

    /**
     * Undo all changes made since the map was created or since last commit or
     * abort.
     */
    public void abort() {
        super.clear();
    }

    /**
     * Clear this map (an anti-object is inserted for each entry present in the
     * backed map).
     */
    @Override
    public void clear() {
        super.clear();

        // Add an anti-entry for each key represented in the
        // parent scope.
        //
        Iterator<Object> itor = m_base.keySet().iterator();
        while (itor.hasNext()) {
            super.put(itor.next(), s_noObject);
        }
    }

    /**
     * Commit all changes made since the map was created or since last commit or
     * abort. All changes are propagated to the backing map.
     */
    public void commit() {
        Iterator<?> itor = super.entrySet().iterator();
        while (itor.hasNext()) {
            @SuppressWarnings("unchecked")
            Map.Entry<Object, Object> e = (Map.Entry<Object, Object>) itor.next();
            Object key = e.getKey();
            Object val = e.getValue();
            if (val == s_noObject) {
                m_base.remove(key);
            } else {
                m_base.put(key, val);
            }
        }
        super.clear();
    }

    @Override
    public boolean containsKey(Object key) {
        Object v = super.get(key);
        if (v != null) {
            return v != s_noObject;
        }

        return super.containsKey(key) || m_base.containsKey(key);
    }

    @Override
    public boolean containsValue(Object val) {
        Iterator<?> itor = getValueIterator();
        while (itor.hasNext()) {
            Object v = itor.next();
            if (v == val || v != null && v.equals(val)) {
                return true;
            }
        }
        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Map.Entry<Object, Object>> entrySet() {
        if (m_entrySet == null) {
            m_entrySet = new AbstractSet<Object>() {
                @Override
                public boolean contains(Object k) {
                    return TransactionalMap.this.containsKey(k);
                }

                @Override
                public Iterator<Object> iterator() {
                    return (Iterator<Object>) TransactionalMap.this.getEntryIterator();
                }

                @Override
                public int size() {
                    return TransactionalMap.this.size();
                }
            };
        }
        return (Set<java.util.Map.Entry<Object, Object>>) m_entrySet;
    }

    @Override
    public Object get(Object key) {
        Object val = super.get(key);
        if (val == s_noObject) {
            val = null;
        } else if (val == null && !super.containsKey(key)) {
            val = m_base.get(key);
        }
        return val;
    }

    @Override
    public boolean isEmpty() {
        return size() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<Object> keySet() {
        if (m_keySet == null) {
            m_keySet = new AbstractSet<Object>() {
                @Override
                public boolean contains(Object k) {
                    return TransactionalMap.this.containsKey(k);
                }

                @Override
                public Iterator<Object> iterator() {
                    return (Iterator<Object>) TransactionalMap.this.getKeyIterator();
                }

                @Override
                public int size() {
                    return TransactionalMap.this.size();
                }
            };
        }
        return (Set<Object>) m_keySet;
    }

    @Override
    public Object put(Object key, Object value) {
        Object old = get(key);
        super.put(key, value);
        return old;
    }

    @Override
    public void putAll(Map<?, ?> t) {
        super.putAll(t);
    }

    @Override
    public Object remove(Object key) {
        Object val = super.get(key);
        if (val == s_noObject) {
            //
            // Already removed
            //
            return null;
        }

        Object bval = m_base.get(key);
        if (bval == null && !m_base.containsKey(key)) {
            // Not present in base
            //
            if (val != null || super.containsKey(key)) {
                super.remove(key);
            }
            return val;
        }

        if (val == null && !super.containsKey(key)) {
            val = bval;
        }

        super.put(key, s_noObject);
        return val;
    }

    @Override
    public int size() {
        int sz = m_base.size();
        int psz = super.size();

        if (sz == 0) {
            return psz;
        }
        if (psz == 0) {
            return sz;
        }

        Iterator<?> itor = super.entrySet().iterator();

        // Decrease counter for entries present in both maps.
        //
        while (itor.hasNext()) {
            @SuppressWarnings("unchecked")
            Map.Entry<Object, Object> me = (Map.Entry<Object, Object>) itor.next();

            Object val = me.getValue();
            if (val == s_noObject) {
                --sz;
            } else if (!m_base.containsKey(me.getKey())) {
                ++sz;
            }
        }
        return sz;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<Object> values() {
        if (m_valueColl == null) {
            m_valueColl = new AbstractCollection<Object>() {
                @Override
                public boolean contains(Object v) {
                    return TransactionalMap.this.containsValue(v);
                }

                @Override
                public Iterator<Object> iterator() {
                    return (Iterator<Object>) TransactionalMap.this.getValueIterator();
                }

                @Override
                public int size() {
                    return TransactionalMap.this.size();
                }
            };
        }
        return (Collection<Object>) m_valueColl;
    }

    private Set<Object> superKeySet() {
        return super.keySet();
    }

    protected Iterator<?> getEntryIterator() {
        return new EntryIterator();
    }

    protected Iterator<?> getKeyIterator() {
        return new KeyIterator();
    }

    protected Iterator<?> getValueIterator() {
        return new ValueIterator();
    }
}