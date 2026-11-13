package integrations

// abandonClaims simulates the process exit that releases a writer's claims: it
// closes each entry lock the writer holds and forgets them, nothing more.
func abandonClaims(w *ChangeWriter) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for file, claim := range w.claims {
		_ = claim.Close()
		delete(w.claims, file)
	}
}
