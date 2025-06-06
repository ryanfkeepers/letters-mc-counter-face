package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strings"

	"github.com/alcionai/clues/clog"
	"github.com/alcionai/clues/cluerr"
	"github.com/pawelszydlo/humanize"
	"github.com/puzpuzpuz/xsync/v4"
	"github.com/spf13/cobra"
)

var (
	flagValSwap       []string
	flagValRemove     []string
	flagValRemoveHTML bool
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

Example: count -swapNgram=th,ð -removeWord=the ~/corpus/alice_in_wonderland.txt

Caveats:

As a simplification, assumes swaps always maintain the same
count of letters in a word, or reduces them.  Increasing the
letter count (ex: -s=e,ea) will cause stats issues in the 
forth letters column.

Currently strips all non-ascii characters during the alpha-
numeric corpus normalization.

The RemoveHTML flag is a low-effort attempt and assumes all
words beginning or ending in angle brackets (<>) can be removed.
This is, of course, faulty.  But sufficient for simple use cases.`,
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

	flags.BoolVarP(
		&flagValRemoveHTML,
		"removeHTML",
		"w",
		false,
		"removes any words that might be part of an html element. ex -removeHTML",
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
	// all text stats with no modifications
	count     *xsync.Counter
	universal *xsync.Map[string, *xsync.Counter]

	// text stats with only swapped parts
	countSwapped *xsync.Counter
	swapped      *xsync.Map[string, *xsync.Counter]

	// text stats with only removed parts
	countRemoved *xsync.Counter
	removed      *xsync.Map[string, *xsync.Counter]

	// text stats with both swapped and removed parts.
	countBoth *xsync.Counter
	both      *xsync.Map[string, *xsync.Counter]
}

func makeStats() stats {
	return stats{
		count:        xsync.NewCounter(),
		universal:    xsync.NewMap[string, *xsync.Counter](),
		countSwapped: xsync.NewCounter(),
		swapped:      xsync.NewMap[string, *xsync.Counter](),
		countRemoved: xsync.NewCounter(),
		removed:      xsync.NewMap[string, *xsync.Counter](),
		countBoth:    xsync.NewCounter(),
		both:         xsync.NewMap[string, *xsync.Counter](),
	}
}

type nGramSwap struct {
	from, to string
}

type handler struct {
	removeWords map[string]struct{}
	swapNGrams  []nGramSwap
	removeHTML  bool
	words       stats
	letters     stats
}

func newHandler() *handler {
	return &handler{
		removeWords: map[string]struct{}{},
		swapNGrams:  []nGramSwap{},
		removeHTML:  false,
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

		h.swapNGrams = append(h.swapNGrams, nGramSwap{
			from: parts[0],
			to:   parts[1],
		})
	}

	for _, remove := range flagValRemove {
		h.removeWords[remove] = struct{}{}
	}

	h.removeHTML = flagValRemoveHTML

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

	print(h.words, "words", 10, os.Stdout)

	fmt.Println(" ")

	print(h.letters, "letters", 0, os.Stdout)

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
		curr, currBroken = normalize(scanner.Text(), h.removeHTML)
	}

	// and one last call to catch the final line
	h.processLine(ctx, curr)

	return nil
}

var (
	keepCharsRE          = regexp.MustCompile(`[^a-zA-Z0-9 ]+`)
	keepCharsAndAnglesRE = regexp.MustCompile(`[^a-zA-Z0-9 <>]+`)
	removeHTMLRE         = regexp.MustCompile(` ?</?[a-zA-Z0-9]+> ?`)
)

// lowers and strips most non-alpha-numeric characters.
func normalize(
	ln string,
	removeHTML bool,
) (
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

	if removeHTML {
		// prereduction makes it easier to isolate html elements
		ln = keepCharsAndAnglesRE.ReplaceAllString(ln, "")
		ln = removeHTMLRE.ReplaceAllString(ln, "")
	}

	ln = keepCharsRE.ReplaceAllString(ln, "")

	return strings.Fields(ln), broken
}

func (h *handler) processLine(
	ctx context.Context,
	ln []string,
) {
	for _, word := range ln {
		// swapped characters
		swapped := word

		for _, swap := range h.swapNGrams {
			swapped = strings.ReplaceAll(word, swap.from, swap.to)
		}

		_, remove := h.removeWords[word]

		// count all words
		inc(&h.words, word, swapped, remove)

		// count all characters in the raw word
		for _, char := range word {
			inc(&h.letters, string(char), "", remove)
		}

		// count all characters in the swapped wordset
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
		// only counting raw additions prevents double
		// counting of characters.
		stats.count.Inc()
		incX(stats.universal, raw)

		if !removed {
			incX(stats.removed, raw)
		} else {
			// only counting raw additions prevents double
			// counting of characters.
			stats.countRemoved.Inc()
		}

	}

	if len(swapped) > 0 {
		stats.countSwapped.Inc()
		incX(stats.swapped, swapped)

		if !removed {
			stats.countBoth.Inc()
			incX(stats.both, swapped)
		}
	}
}

// incX ensures the xsync count is populated and incs
// the given key.
func incX(
	m *xsync.Map[string, *xsync.Counter],
	k string,
) {
	if len(k) == 0 {
		return
	}

	v, ok := m.Load(k)
	if !ok {
		v = xsync.NewCounter()
		m.Store(k, v)
	}

	v.Inc()
}

type unit struct {
	v string
	n int
}

func print(
	stats stats,
	title string,
	top int,
	w io.Writer,
) {
	var (
		u = toUnitSlice(stats.universal)
		r = toUnitSlice(stats.removed)
		s = toUnitSlice(stats.swapped)
		b = toUnitSlice(stats.both)
	)

	if top > 0 {
		if len(u) > top {
			u = u[:top]
		}

		if len(r) > top {
			r = r[:top]
		}

		if len(s) > top {
			s = s[:top]
		}

		if len(b) > top {
			b = b[:top]
		}
	}

	longest := max(len(u), len(s), len(r), len(b))

	writeLn(w, title)
	writeLn(
		w,
		"|  "+
			addCellHeader("raw", stats.count.Value())+
			addCellHeader("removed", stats.count.Value()-stats.countRemoved.Value())+
			addCellHeader("swapped", stats.countSwapped.Value())+
			addCellHeader("both", stats.countBoth.Value())+
			"|",
	)
	writeLn(w, "|---|---|---|---|---|")

	for i := range longest {
		writeLn(
			w,
			fmt.Sprintf("| %2d ", i)+
				addCellUnit(i, u, stats.count.Value())+
				addCellUnit(i, r, stats.count.Value()-stats.countRemoved.Value())+
				addCellUnit(i, s, stats.countSwapped.Value())+
				addCellUnit(i, b, stats.countBoth.Value())+
				"|",
		)
	}
}

func toUnitSlice(counter *xsync.Map[string, *xsync.Counter]) []unit {
	result := []unit{}

	counter.Range(func(key string, value *xsync.Counter) bool {
		result = append(result, unit{key, int(value.Value())})
		return true
	})

	slices.SortFunc(result, func(a, b unit) int {
		diff := b.n - a.n
		if diff != 0 {
			return diff
		}

		return strings.Compare(a.v, b.v)
	})

	return result
}

func writeLn(
	w io.Writer,
	ln string,
) {
	fmt.Fprint(w, ln+"\n")
}

func addCellHeader(
	title string,
	total int64,
) string {
	return fmt.Sprintf("| %s (%s) ", title, human(total))
}

func addCellUnit(
	i int,
	sl []unit,
	total int64,
) string {
	if len(sl) <= i {
		return "|  "
	}

	u := sl[i]

	return fmt.Sprintf(
		"| %5s (%6s, %2.2f%%) ",
		u.v,
		human(u.n),
		(float64(u.n)/float64(total))*100,
	)
}

type inter interface {
	int | int64
}

func human[Z inter](z Z) string {
	hzr, _ := humanize.New("en")
	return hzr.SiPrefixFast(float64(z))
}
