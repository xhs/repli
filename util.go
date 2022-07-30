package repli

import "regexp"

func CompileRegExpPatterns(regExpPatterns []string) []*regexp.Regexp {
	var patterns []*regexp.Regexp
	for _, pattern := range regExpPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			panic(err)
		}

		patterns = append(patterns, re)
	}

	return patterns
}
