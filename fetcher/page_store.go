package fetcher

type PageStore interface {
	Save(domain string, urlpath string, page string) error
}
