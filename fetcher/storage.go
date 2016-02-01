package fetcher

type Storage interface {
	Save(html []byte, domain string, urlpath string) error
}
