package utils

/*************
* 配置读取函数，配置格式为ini格式
*
*****************/
import (
	"bufio"
	log "github.com/kdar/factorlog"
	"os"
	"strings"
)

func ReadConfig(cfgFile string) (map[string]map[string]string, error) {
	fin, err := os.Open(cfgFile)
	config := make(map[string]map[string]string)
	if err != nil {
		log.Fatalln(err)
	} else {
		config[""] = make(map[string]string)
		var section = ""
		scanner := bufio.NewScanner(fin)
		//逐行读取
		for scanner.Scan() {
			line := strings.Trim(scanner.Text(), " ")
			if line == "" || line[0] == ';' || line[0] == '#' {
				//这行是注释，跳过
				continue
			}
			lSqr := strings.Index(line, "[")
			rSqr := strings.Index(line, "]")
			if lSqr == 0 && rSqr == len(line)-1 {
				section = line[lSqr+1 : rSqr]
				_, ok := config[section]
				if !ok {
					config[section] = make(map[string]string)
				}
				continue
			}

			equalPos := strings.Index(line, "=")
			if equalPos > 0 {
				key := strings.Trim(line[0:equalPos], " ")
				val := strings.Trim(line[equalPos+1:], " ")
				config[section][key] = val
			}
		}
		fin.Close()
	}
	return config, err
}
