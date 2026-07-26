import { Component, type ErrorInfo, type ReactNode } from 'react';
import { Pressable, StyleSheet, Text, View } from 'react-native';

/**
 * The last line of defence: whatever a render throws, the visitor gets words and a way out instead
 * of a blank document.
 *
 * Measured, not hypothetical. With the collections service answering 500 with a JSON error body,
 * `setItems` used to take that OBJECT, `items.some(...)` threw a TypeError, and — with no boundary
 * anywhere — React unmounted the root: `#root` innerHTML length 0, verified in the browser. The
 * read guards its own shape now (App.tsx checks Array.isArray); this makes sure the next such bug
 * costs a paragraph rather than the whole screen.
 *
 * A class component on purpose: `componentDidCatch` has no hook equivalent.
 */
export class ErrorBoundary extends Component<{ children: ReactNode }, { failed: boolean }> {
  state = { failed: false };

  static getDerivedStateFromError() {
    return { failed: true };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('collections-ui hit an unhandled error', error, info.componentStack);
  }

  render() {
    if (!this.state.failed) return this.props.children;
    return (
      <View style={styles.screen}>
        <View style={styles.card}>
          <Text style={styles.title}>Something answered unexpectedly</Text>
          <Text style={styles.body}>
            One of the services replied in a way this screen did not expect. Nothing has been lost —
            try again, and if it keeps happening the service is down.
          </Text>
          <Pressable style={styles.button} onPress={() => window.location.reload()}>
            <Text style={styles.buttonText}>Try again</Text>
          </Pressable>
        </View>
      </View>
    );
  }
}

const styles = StyleSheet.create({
  screen: { flex: 1, alignItems: 'center', justifyContent: 'center', backgroundColor: '#101418' },
  card: {
    width: 420, maxWidth: '92%', padding: 24, borderRadius: 12,
    backgroundColor: '#1a2027', gap: 12,
  },
  title: { fontSize: 20, fontWeight: '700', color: '#e8edf2' },
  body: { fontSize: 13, color: '#8a97a3' },
  button: {
    backgroundColor: '#7fd1b9', borderRadius: 8, paddingHorizontal: 16,
    paddingVertical: 10, alignItems: 'center', alignSelf: 'flex-start',
  },
  buttonText: { color: '#101418', fontWeight: '700' },
});
