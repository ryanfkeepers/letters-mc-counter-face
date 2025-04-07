package main

import (
	"bufio"
	"context"
	"os"
	"regexp"
	"strings"

	"github.com/alcionai/clues/clog"
	"github.com/alcionai/clues/cluerr"
	"github.com/spf13/cobra"
)

var (
	flagValSwap   []string
	flagValRemove []string
)

func newRoot(h *handler) *cobra.Command {
	root := &cobra.Command{
		Use:   "count",
		Short: "count all letters and words in the provided corpuses",
		Long: `count provides a quick overview of letters and word counts
as an aggregate of all provided corpuses.  In addition, it
extends functionality with letter-set swapping (ex: th->ð),
and word slicing (ex: ignore all "the").

Accepts a list of filepaths to .txt files as arguments.

Example: count -sswapNgram=th,ð -removeWord=the ~/corpus/alice_in_wonderland.txt`,
		Args: cobra.MinimumNArgs(1),
		RunE: h.run,
	}

	flags := root.Flags()

	flags.StringArrayVarP(
		&flagValSwap,
		"swapNgram",
		"s",
		[]string{},
		"a comma separated pair of to and from letters. ex -s=th,ð",
	)

	flags.StringSliceVarP(
		&flagValRemove,
		"removeWord",
		"r",
		[]string{},
		"a comma separated list of words to remove entirely.  ex -r=the",
	)

	return root
}

func main() {
	ctx := clog.Init(context.Background(), clog.Settings{})

	defer func() {
		clog.Flush(ctx)
	}()

	err := newRoot(newHandler()).ExecuteContext(ctx)
	if err != nil {
		os.Exit(1)
	}
}

type stats struct {
	// universal count of the tracked thing
	count        int
	countRemoved int

	// all text stats with no modifications
	complete map[string]int

	// text stats with only swapped parts
	swapped map[string]int

	// text stats with only removed parts
	removed map[string]int

	// text stats with both swapped and removed parts.
	both map[string]int
}

func makeStats() stats {
	return stats{
		complete: map[string]int{},
		swapped:  map[string]int{},
		removed:  map[string]int{},
		both:     map[string]int{},
	}
}

type handler struct {
	removeWords map[string]struct{}
	swapNGrams  map[string]string
	words       stats
	letters     stats
}

func newHandler() *handler {
	return &handler{
		removeWords: map[string]struct{}{},
		swapNGrams:  map[string]string{},
		words:       makeStats(),
		letters:     makeStats(),
	}
}

// post processing of flag inputs after cobra has engaged the command
// and processed the flags.  This sets everything up for usage in scanning.
func (h *handler) parseFlags() error {
	for _, swap := range flagValSwap {
		parts := strings.Split(
			strings.ToLower(swap),
			",",
		)

		if len(parts) != 2 {
			return cluerr.New("improperly formed swapNGram: only one ',' expected").
				With("input", swap)
		}

		h.swapNGrams[parts[0]] = parts[1]
	}

	for _, remove := range flagValRemove {
		h.removeWords[remove] = struct{}{}
	}

	return nil
}

func (h *handler) run(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	if err := h.parseFlags(); err != nil {
		return cluerr.WrapWC(ctx, err, "parsing flags")
	}

	// precheck all files for validity
	for _, arg := range args {
		if !strings.HasSuffix(arg, ".txt") {
			return cluerr.NewWC(ctx, "must be .txt: "+arg)
		}

		_, err := os.Stat(arg)
		if err != nil {
			return cluerr.WrapWC(ctx, err, "checking file: "+arg)
		}
	}

	// aggregate all stats per file
	for _, arg := range args {
		if err := h.runFile(ctx, arg); err != nil {
			return cluerr.Wrap(err, "executing command")
		}
	}

	return nil
}

func (h *handler) runFile(
	ctx context.Context,
	filePath string,
) error {
	f, err := os.Open(filePath)
	if err != nil {
		return cluerr.WrapWC(ctx, err, "opening file: "+filePath)
	}

	defer f.Close()

	err = h.processFile(ctx, f)

	return cluerr.WrapWC(
		ctx,
		err,
		"processing file: "+filePath,
	).OrNil()
}

func (h *handler) processFile(
	ctx context.Context,
	f *os.File,
) (err error) {
	defer func() {
		r := recover()
		if r != nil {
			clog.CtxErr(ctx, r.(error)).Error("CAUGHT PANIC")
			err = r.(error)
		}
	}()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	// prev and current represent lines of text scanned
	// in by bufio.  We hold both lines in order to mediate
	// words split in printing via -.
	var prev, curr []string
	var prevBroken, currBroken bool

	for scanner.Scan() {
		if len(prev) > 0 {
			// assume we need to stitch together a broken word
			if prevBroken && len(curr) > 0 {
				prev[len(prev)-1] = prev[len(prev)-1] + curr[0]
				curr = curr[1:]
			}

			h.processLine(ctx, prev)
		}

		prev = curr
		prevBroken = currBroken
		curr, currBroken = normalize(scanner.Text())
	}

	return nil
}

var keepCharsRE = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)

// lowers and strips most non-alpha-numeric characters.
func normalize(ln string) (
	[]string, // the revised text
	bool, // whether the original text ended in a dash-broken word.
) {
	// first to ensure we catch broken words.
	ln = strings.TrimSpace(ln)

	if len(ln) == 0 {
		return nil, false
	}

	broken := len(ln) > 1 &&
		strings.HasSuffix(ln, "-") &&
		string(ln[len(ln)-2]) != ""

	ln = strings.ToLower(ln)
	ln = strings.TrimSpace(ln)
	ln = keepCharsRE.ReplaceAllString(ln, "")

	return strings.Fields(ln), broken
}

func (h *handler) processLine(
	ctx context.Context,
	ln []string,
) {
	for _, word := range ln {
		// universal set
		h.words.count++
		h.words.complete[word] = h.words.complete[word] + 1

		// swapped characters
		swapped := word

		for from, to := range h.swapNGrams {
			swapped = strings.ReplaceAll(word, from, to)
		}

		_, remove := h.removeWords[word]

		inc(&h.words, word, swapped, remove)

		for _, char := range word {
			inc(&h.letters, string(char), "", remove)
		}

		for _, char := range swapped {
			inc(&h.letters, "", string(char), remove)
		}
	}
}

// inc mutates the stats maps to increment all values
func inc(
	stats *stats,
	raw, swapped string,
	removed bool,
) {
	if len(raw) > 0 {
		// only counting raw additions prevent double
		// counting of characters.
		stats.count++
		stats.complete[raw] = stats.complete[raw] + 1
	}

	if len(swapped) > 0 {
		stats.swapped[swapped] = stats.swapped[swapped] + 1
	}

	if removed {
		if len(raw) > 0 {
			// only counting raw additions prevent double
			// counting of characters.
			stats.countRemoved++
		}
	} else {
		if len(swapped) > 0 {
			stats.both[swapped] = stats.both[swapped] + 1
		}
	}
}
