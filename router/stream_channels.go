package router

// streamChannels returns the exact Redis channels this request publishes to.
// Combined channels are de-duplicated because every symbol in the combined set
// resolves to the same channel.
func (rt *StonksRuntime) streamChannels() []string {
	seen := make(map[string]struct{})
	channels := make([]string, 0, len(rt.cfg.subscriptions)*len(rt.cfg.tickers))
	for _, sub := range rt.cfg.subscriptions {
		for _, symbol := range rt.cfg.tickers {
			channel := rt.publishChannel(sub, symbol)
			if _, ok := seen[channel]; ok {
				continue
			}
			seen[channel] = struct{}{}
			channels = append(channels, channel)
		}
	}
	return channels
}
