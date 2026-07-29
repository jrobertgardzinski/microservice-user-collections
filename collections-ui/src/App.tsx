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

import { isResolvable, refKey, resolveMeme, type Resolution } from './refs';

// The browser calls both services cross-origin — that is the point of this UI: it exercises the
// CORS edge of user-collections (and rides security's existing one).
//
// Addresses come from the DEPLOYMENT first (window.__PORTAL_CONFIG__, written by ui-config.sh at
// container start and loaded by index.html before this module), then from the VITE_* values baked
// at build time, then from compose's host-published ports. The chain exists because Vite bakes its
// values into the bundle: one image carried one set of addresses, so the page served from a cluster
// still aimed the browser at the developer's laptop.
declare global {
  interface Window {
    __PORTAL_CONFIG__?: { securityUrl?: string; collectionsUrl?: string; memesUrl?: string };
  }
}

const deployed = (key: 'securityUrl' | 'collectionsUrl' | 'memesUrl'): string | undefined =>
  typeof window === 'undefined' ? undefined : window.__PORTAL_CONFIG__?.[key];

const SECURITY = deployed('securityUrl') ?? import.meta.env.VITE_SECURITY_URL ?? 'http://localhost:8080';
const COLLECTIONS = deployed('collectionsUrl') ?? import.meta.env.VITE_COLLECTIONS_URL ?? 'http://localhost:8092';

// The gallery is the ONE call this UI makes same-origin (default: ''), through the nginx proxy in
// front of the bundle — see nginx.conf.template. Cross-origin it would be blocked and every check would
// come back "could not check", which is safe but useless.
const MEMES = deployed('memesUrl') ?? import.meta.env.VITE_MEMES_URL ?? '';

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
  // "we could not find out" is a THIRD state next to "here is your list" and "your list is empty" —
  // the same discipline refs.ts already applies to a single reference (gone vs unknown), now for the
  // whole list. Collapsing them printed "Nothing saved yet" over a service that was merely restarting.
  const [listError, setListError] = useState<string | null>(null);
  const [itemType, setItemType] = useState('meme');
  const [itemId, setItemId] = useState('');
  const [busy, setBusy] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  // what we know about each saved reference. Read-only knowledge: nothing in this component ever
  // deletes anything because of what lands here — see refs.ts for why that is a hard rule
  const [resolutions, setResolutions] = useState<Record<string, Resolution>>({});

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
      if (!response.ok) {
        // a status is the service talking about ITSELF; it says nothing about what is in the
        // collection, so the list is not touched and the screen says which it is
        setListError(`The collections service answered ${response.status}.`);
        return;
      }
      // an answer with no body (or a body that is not a list) used to make json() throw inside the
      // try, leave items at null, and park the screen on a spinner that never stopped
      const body: unknown = await response.json().catch(() => null);
      if (!Array.isArray(body)) {
        setListError('The collections service answered with something that is not a list.');
        return;
      }
      setItems(body as Ref[]);
      setListError(null);
    } catch {
      setListError('Collections service unreachable.');
    }
  }, [authorized, signOut]);

  useEffect(() => { load(); }, [load]);

  // Resolve every reference we know how to resolve. Deliberately a READ: it fills in a badge and,
  // for a reference the gallery says it does not have, an explicit repair button. It never issues
  // a DELETE — 26 of the live stack's 91 memes answer 404 while their row exists (AUDYT-2026-07-26
  // #4), so an auto-repair here would quietly eat a quarter of everyone's favourites.
  useEffect(() => {
    if (!items) return;
    let current = true;
    for (const ref of items) {
      if (!isResolvable(ref.itemType)) continue;
      const key = refKey(ref);
      setResolutions((prev) => (key in prev ? prev : { ...prev, [key]: { state: 'checking' } }));
      resolveMeme(MEMES, ref.itemId).then((resolution) => {
        // functional update, never a snapshot: several of these land out of order, and a
        // whole-object rollback is exactly how a UI ends up showing a set the server never had
        if (current) setResolutions((prev) => ({ ...prev, [key]: resolution }));
      });
    }
    return () => { current = false; };
  }, [items]);

  const save = async () => {
    const type = itemType.trim();
    const id = itemId.trim();
    if (!type || !id) {
      setNotice('An item needs both a type and an id.');
      return;
    }
    setNotice(null);
    setBusy(true);
    try {
      // BOTH segments encoded. Raw concatenation meant an id containing a slash (anybody pasting a
      // URL) addressed a different endpoint entirely, and a '?' or '#' truncated the request — the
      // server then answered honestly about a path nobody meant to call
      const response = await authorized(
        `/${encodeURIComponent(type)}/${encodeURIComponent(id)}`, 'PUT');
      if (response.status === 401) {
        signOut();
        return;
      }
      if (!response.ok) {
        // the typed value STAYS in the field — it was the only copy, and clearing it before the
        // server had accepted anything is how the same id got retyped over and over
        setNotice(response.status === 400
          ? 'That type or id was refused — check it (ids are at most 128 characters).'
          : `Not saved — the collections service answered ${response.status}.`);
        return;
      }
      setItemId('');            // cleared only now, on a confirmed save
      await load();
    } catch {
      setNotice('Collections service unreachable — nothing was saved.');
    } finally {
      setBusy(false);
    }
  };

  const remove = async (ref: Ref) => {
    setNotice(null);
    try {
      const response = await authorized(
        `/${encodeURIComponent(ref.itemType)}/${encodeURIComponent(ref.itemId)}`, 'DELETE');
      if (response.status === 401) {
        signOut();
        return;
      }
      // 404 is a SUCCESS: the entry is not there, which is exactly what the click asked for.
      // Removing is idempotent, and reporting "it failed" for an entry that is already gone leaves
      // a button that can never succeed
      if (!response.ok && response.status !== 404) {
        setNotice(`Not removed — the collections service answered ${response.status}.`);
        return;
      }
      await load();
    } catch {
      setNotice('Collections service unreachable — nothing was removed.');
    }
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
          <Pressable style={styles.button} onPress={save} disabled={busy}>
            {busy ? <ActivityIndicator color="#101418" />
                  : <Text style={styles.buttonText}>Save</Text>}
          </Pressable>
        </View>
        {notice && <Text style={styles.notice}>{notice}</Text>}
        {listError !== null ? (
          // an unanswered question, rendered as one: no "nothing saved yet", and a way to ask again
          // without reaching for F5
          <View style={styles.errorBox}>
            <Text style={styles.notice}>{listError}</Text>
            <Text style={styles.hint}>
              This is a service problem, not an empty collection — nothing has been lost.
            </Text>
            <Pressable style={styles.repairButton} onPress={load}>
              <Text style={styles.repairButtonText}>try again</Text>
            </Pressable>
          </View>
        ) : items === null ? (
          <ActivityIndicator color="#7fd1b9" />
        ) : items.length === 0 ? (
          <Text style={styles.empty}>Nothing saved yet — refs land here newest first.</Text>
        ) : (
          <>
            {items.some((ref) => resolutions[refKey(ref)]?.state === 'gone') && (
              <Text style={styles.hint}>
                Some entries point at memes the gallery says it no longer has. Nothing is removed
                for you — decide entry by entry.
              </Text>
            )}
            <ScrollView style={styles.list}>
              {items.map((ref) => (
                <ItemTile
                  key={refKey(ref)}
                  item={ref}
                  resolution={resolutions[refKey(ref)]}
                  onRemove={() => remove(ref)}
                />
              ))}
            </ScrollView>
          </>
        )}
      </View>
    </View>
  );
}

/**
 * One saved reference. The three interesting states are what this component exists for:
 *
 * - `gone` — the gallery answered "I do not have this". The tile renders as UNAVAILABLE and grows
 *   an explicit "remove from favourites" button. It does NOT remove itself. See refs.ts: on the
 *   live stack 26 of 91 memes answer 404 while their row is perfectly alive, so the difference
 *   between offering and doing is the difference between a wrong tile and lost data.
 * - `unknown` — we could not find out (5xx, timeout, offline). That is an ERROR state, not a
 *   verdict about the meme: the entry renders normally with a "couldn't check" note and gets no
 *   repair button at all, because there is nothing to repair as far as anyone knows.
 * - `checking` / absent — nothing to say yet, or nothing we know how to check. Renders plainly.
 */
function ItemTile({ item, resolution, onRemove }: {
  item: Ref;
  resolution: Resolution | undefined;
  onRemove: () => void;
}) {
  const gone = resolution?.state === 'gone';
  return (
    <View style={[styles.itemRow, gone && styles.itemRowGone]}>
      <Text style={[styles.itemType, gone && styles.dimmed]}>{item.itemType}</Text>
      <View style={styles.itemBody}>
        <Text style={[styles.itemId, gone && styles.itemIdGone]}>{item.itemId}</Text>
        {gone && <Text style={styles.unavailable}>unavailable — the gallery no longer has it</Text>}
        {resolution?.state === 'unknown' && (
          <Text style={styles.uncertain}>couldn't check ({resolution.why}) — entry kept</Text>
        )}
        {resolution?.state === 'checking' && <Text style={styles.uncertain}>checking…</Text>}
      </View>
      {gone ? (
        // the repair, and the ONLY repair: a human, one entry, one click
        <Pressable style={styles.repairButton} onPress={onRemove}>
          <Text style={styles.repairButtonText}>remove from favourites</Text>
        </Pressable>
      ) : (
        <Pressable onPress={onRemove}>
          <Text style={styles.remove}>remove</Text>
        </Pressable>
      )}
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
  // "we could not find out" gets its own block with a retry — never the empty-collection sentence
  errorBox: { gap: 6, alignItems: 'flex-start' },
  list: { maxHeight: 320 },
  itemRow: {
    flexDirection: 'row', alignItems: 'center', gap: 10,
    paddingVertical: 8, borderBottomWidth: 1, borderBottomColor: '#242c35',
  },
  itemType: { color: '#7fd1b9', fontSize: 13, width: 90 },
  itemBody: { flex: 1, gap: 2 },
  itemId: { color: '#e8edf2', fontSize: 14 },
  remove: { color: '#f2b8b5', fontSize: 12 },
  link: { color: '#7fd1b9', fontSize: 13 },
  hint: { color: '#8a97a3', fontSize: 12 },
  // an unresolvable reference LOOKS broken and says why — it does not disappear
  itemRowGone: { opacity: 0.75 },
  itemIdGone: { color: '#8a97a3', textDecorationLine: 'line-through' },
  dimmed: { color: '#5c6773' },
  unavailable: { color: '#f2b8b5', fontSize: 11 },
  // "we could not check" is deliberately a NEUTRAL note, not an alarm and not a verdict
  uncertain: { color: '#8a97a3', fontSize: 11, fontStyle: 'italic' },
  repairButton: {
    borderWidth: 1, borderColor: '#f2b8b5', borderRadius: 8,
    paddingHorizontal: 10, paddingVertical: 6,
  },
  repairButtonText: { color: '#f2b8b5', fontSize: 11, fontWeight: '700' },
});
