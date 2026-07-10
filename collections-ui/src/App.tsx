import { useCallback, useEffect, useState } from 'react';
import {
  ActivityIndicator,
  Pressable,
  ScrollView,
  StyleSheet,
  Text,
  TextInput,
  View,
} from 'react-native';

// The browser calls both services cross-origin — that is the point of this UI: it exercises the
// CORS edge of user-collections (and rides security's existing one). Host-published ports by
// default; a deployment overrides via VITE_*_URL at build time.
const SECURITY = import.meta.env.VITE_SECURITY_URL ?? 'http://localhost:8080';
const COLLECTIONS = import.meta.env.VITE_COLLECTIONS_URL ?? 'http://localhost:8092';

type Ref = { itemType: string; itemId: string };

export function App() {
  const [token, setToken] = useState<string | null>(null);
  const [who, setWho] = useState('');
  return token
    ? <Favourites token={token} who={who} signOut={() => setToken(null)} />
    : <SignIn onSignedIn={(t, email) => { setToken(t); setWho(email); }} />;
}

function SignIn({ onSignedIn }: { onSignedIn: (token: string, email: string) => void }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [notice, setNotice] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const signIn = async () => {
    setBusy(true);
    setNotice(null);
    try {
      const response = await fetch(`${SECURITY}/authenticate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });
      if (response.status === 200) {
        const body = await response.json();
        onSignedIn(body.accessToken, email);
      } else if (response.status === 403) {
        setNotice('E-mail not verified yet — click the link in your inbox first.');
      } else if (response.status === 202) {
        setNotice('This account asks for a second factor — sign in via the gallery UI for MFA.');
      } else {
        setNotice('Wrong e-mail or password.');
      }
    } catch {
      setNotice('Security service unreachable.');
    } finally {
      setBusy(false);
    }
  };

  return (
    <View style={styles.screen}>
      <View style={styles.card}>
        <Text style={styles.title}>My collections</Text>
        <Text style={styles.subtitle}>Sign in with your account from the gallery</Text>
        <TextInput
          style={styles.input}
          placeholder="e-mail"
          placeholderTextColor="#5c6773"
          autoCapitalize="none"
          value={email}
          onChangeText={setEmail}
        />
        <TextInput
          style={styles.input}
          placeholder="password"
          placeholderTextColor="#5c6773"
          secureTextEntry
          value={password}
          onChangeText={setPassword}
          onSubmitEditing={signIn}
        />
        {notice && <Text style={styles.notice}>{notice}</Text>}
        <Pressable style={styles.button} onPress={signIn} disabled={busy}>
          {busy ? <ActivityIndicator color="#101418" /> : <Text style={styles.buttonText}>Sign in</Text>}
        </Pressable>
      </View>
    </View>
  );
}

function Favourites({ token, who, signOut }: { token: string; who: string; signOut: () => void }) {
  const [items, setItems] = useState<Ref[] | null>(null);
  const [itemType, setItemType] = useState('meme');
  const [itemId, setItemId] = useState('');
  const [notice, setNotice] = useState<string | null>(null);

  const authorized = useCallback(
    (path: string, method: string = 'GET') =>
      fetch(`${COLLECTIONS}/collections/favourites/items${path}`, {
        method,
        headers: { Authorization: `Bearer ${token}` },
      }),
    [token],
  );

  const load = useCallback(async () => {
    try {
      const response = await authorized('');
      if (response.status === 401) {
        signOut();
        return;
      }
      setItems(await response.json());
    } catch {
      setNotice('Collections service unreachable.');
    }
  }, [authorized, signOut]);

  useEffect(() => { load(); }, [load]);

  const save = async () => {
    if (!itemType.trim() || !itemId.trim()) {
      setNotice('An item needs both a type and an id.');
      return;
    }
    setNotice(null);
    await authorized(`/${itemType.trim()}/${itemId.trim()}`, 'PUT');
    setItemId('');
    load();
  };

  const remove = async (ref: Ref) => {
    await authorized(`/${ref.itemType}/${ref.itemId}`, 'DELETE');
    load();
  };

  return (
    <View style={styles.screen}>
      <View style={styles.card}>
        <View style={styles.headerRow}>
          <Text style={styles.title}>Favourites</Text>
          <Pressable onPress={signOut}>
            <Text style={styles.link}>sign out</Text>
          </Pressable>
        </View>
        <Text style={styles.subtitle}>{who}</Text>
        <View style={styles.addRow}>
          <TextInput
            style={[styles.input, styles.typeInput]}
            placeholder="type"
            placeholderTextColor="#5c6773"
            autoCapitalize="none"
            value={itemType}
            onChangeText={setItemType}
          />
          <TextInput
            style={[styles.input, styles.idInput]}
            placeholder="id"
            placeholderTextColor="#5c6773"
            autoCapitalize="none"
            value={itemId}
            onChangeText={setItemId}
            onSubmitEditing={save}
          />
          <Pressable style={styles.button} onPress={save}>
            <Text style={styles.buttonText}>Save</Text>
          </Pressable>
        </View>
        {notice && <Text style={styles.notice}>{notice}</Text>}
        {items === null ? (
          <ActivityIndicator color="#7fd1b9" />
        ) : items.length === 0 ? (
          <Text style={styles.empty}>Nothing saved yet — refs land here newest first.</Text>
        ) : (
          <ScrollView style={styles.list}>
            {items.map((ref) => (
              <View key={`${ref.itemType}/${ref.itemId}`} style={styles.itemRow}>
                <Text style={styles.itemType}>{ref.itemType}</Text>
                <Text style={styles.itemId}>{ref.itemId}</Text>
                <Pressable onPress={() => remove(ref)}>
                  <Text style={styles.remove}>remove</Text>
                </Pressable>
              </View>
            ))}
          </ScrollView>
        )}
      </View>
    </View>
  );
}

const styles = StyleSheet.create({
  screen: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: '#101418' },
  card: {
    width: 420, maxWidth: '92%', padding: 24, borderRadius: 12,
    backgroundColor: '#1a2027', gap: 12,
  },
  headerRow: { flexDirection: 'row', justifyContent: 'space-between', alignItems: 'baseline' },
  title: { fontSize: 24, fontWeight: '700', color: '#e8edf2' },
  subtitle: { fontSize: 13, color: '#8a97a3' },
  input: {
    backgroundColor: '#101418', color: '#e8edf2', borderRadius: 8,
    paddingHorizontal: 12, paddingVertical: 10, fontSize: 14,
  },
  addRow: { flexDirection: 'row', gap: 8, alignItems: 'center' },
  typeInput: { width: 110 },
  idInput: { flex: 1 },
  button: {
    backgroundColor: '#7fd1b9', borderRadius: 8, paddingHorizontal: 16,
    paddingVertical: 10, alignItems: 'center',
  },
  buttonText: { color: '#101418', fontWeight: '700' },
  notice: { color: '#f2b8b5', fontSize: 13 },
  empty: { color: '#8a97a3', fontSize: 13, fontStyle: 'italic' },
  list: { maxHeight: 320 },
  itemRow: {
    flexDirection: 'row', alignItems: 'center', gap: 10,
    paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: '#242c35',
  },
  itemType: { color: '#7fd1b9', fontSize: 13, width: 90 },
  itemId: { color: '#e8edf2', fontSize: 14, flex: 1 },
  remove: { color: '#f2b8b5', fontSize: 12 },
  link: { color: '#7fd1b9', fontSize: 13 },
});
