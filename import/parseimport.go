package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	_ "net/http/pprof" 

	_ "github.com/ClickHouse/clickhouse-go"
	"github.com/bits-and-blooms/bloom"
	"github.com/sirupsen/logrus"
)

// leohuang hivesec 20240123
/*
CREATE TABLE sgk.urluserpass
(
	`id` Int64,
    `username` String,
    `password` String,
    `url` String,
    `source` String,
    `sourcedate` Date,
    INDEX idx_username_password_url (username, password, url) TYPE minmax GRANULARITY 8192,
    INDEX idx_username (username) TYPE minmax GRANULARITY 8192,
    INDEX idx_url (url) TYPE minmax GRANULARITY 8192
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(sourcedate)  -- 以月份为单位进行分区
PRIMARY KEY (id, username)
ORDER BY (id, username, sourcedate)
SETTINGS index_granularity = 8192;





*/

type LinesFileInfo struct {
	FileName   string
	UpdateDate string
	LinesList  []string
}

type SgkBrowserDataRecord struct {
	Username  string
	Password  string
	URL       string
	Separator string
}

type DbDataRecord struct {
	ID         int64
	Username   string
	Password   string
	URL        string
	Source     string
	SourceDate string
}

type DbDataRecordList []DbDataRecord

var (
	mailPattern                   *regexp.Regexp
	urlPattern                    *regexp.Regexp
	nonStandardHostUrlPattern     *regexp.Regexp
	fileUrlPattern                *regexp.Regexp
	browserExtUrlPattern          *regexp.Regexp
	nonStandardProtocalUrlPattern *regexp.Regexp
	nonStandardUrlLastPattern     *regexp.Regexp
	androidPattern1               *regexp.Regexp
	androidPattern2               *regexp.Regexp
	appPattern                    *regexp.Regexp
	// softPattern                   *regexp.Regexp
	// normalUrlSuffix               *regexp.Regexp
	separatorList          []string
	invalidCharsInUsername []string
	appStrList             []string
	// separatorMap                  map[string]int
	// invalidCharsInUsernameMap     map[string]int
	// appStrMap                     map[string]int
	linesChunk  int
	logger      *logrus.Logger
	bloomFilter *bloom.BloomFilter
)

func initBloom() {
	// 初始化不容过滤器，80亿空间，误报率0.01%
	expectedInsertions := uint(8000000000)
	errRate := 0.001
	bloomFilter = bloom.NewWithEstimates(expectedInsertions, errRate)
}

func initEnv() {

	// 日志设置
	logger = logrus.New()
	logger.SetLevel(logrus.DebugLevel)
	// logger.SetFormatter(&logrus.JSONFormatter{})
	logger.SetFormatter(&logrus.TextFormatter{
		FullTimestamp: true,
	})
	logger.SetOutput(os.Stdout)

	linesChunk = 5000000
	separatorList = []string{"|", " ", ":", ";"}
	invalidCharsInUsername = []string{"!", "#", "$", "%", "^", "&", "*", "?", "+", "="}
	appStrList = []string{"[Edge]", "[Chrome]", "[YandexBrowser]", "[Browser]", "[Atom]", "[Profile_"}

	// 正则表达式
	// 邮箱正则表达式
	mailPattern = regexp.MustCompile(`\b[\w.%+-]+@[\w.-]+\.[a-zA-Z]{2,}\b`)

	// 浏览器应用正则表达式
	/*
		https://mail@RD12345:Application::Microsoft_[Edge]_Profile:1:===============
		https://ohmulti555@gmail.com:011223:Application::Microsoft_[Edge]_Profile:1
		https://yx9N6uS@ecNEjUL:Application::Google_[Chrome]_Default:===============
		https://yuzziezy@gmail.com:holawey123:Application::Google_[Chrome]_Default

		https://husseinadnan hassen123A123:Google_[Chrome]_Default
		Microsoft_[Edge]_Default:https://passbook.epfindia.gov.in/MemberPassBook/Login:999286938996:Pracatio
		http://10gbpsdz.link:80:j34271267@7:Microsoft_[Edge]_Default
		marinoc@netflix.com 11223344:Google_[Chrome]_Default
		marinoc12@netflix.com 11223344:Google_[Chrome]_Default
		https://vi-vn.facebook.com:0383575806:01683575806:CocCoc_[Browser]_Profile:1
		https://three.mlmone.click/register:Dimakst:4325an6325:Mail.Ru_[Atom]_Default
		https://giaoan.violet.vn:thiennhai72:0982556267:CocCoc_[Browser]_Profile:1
		https://robertharrison508@gmail.com:::Gulfsream12006469:Chrome_[Profile_1]
		https://yytyybynyyyrthgttttygtyr:Browser::Chrome_[Profile_6]:---------------
		https:/::deniz.medyatanitim@gmail.com:694570dT27718:Microsoft_[Edge]_Profile
		hrdypunk@hotmail.com:esagente01 Google_[Chrome]_Profile:1
		https://zooniverse.org/projects:KyawZaweLin:tak6akzl:Google_[Chrome]_Profile:45.
		http://bosslike.ru/login::samoha9898@mail.ru:s777amoha:Google_[Chrome]_Backup
		http://ets2mp.com::samoha9898@mail.ru:s777amoha:Google_[Chrome]_Backup:Default
		https://kude.com2082/user/database/create:::lulus:4rifinku:Google_[Chrome]
		https://mail.ru:maxchizh24@mail.ru:maxlenovo:Yandex_[YandexBrowser]_Default
		ide:gas picim na:max:idegas123:Google_[Chrome]_Profile:24https://play.prodigygame.com:DylanM15594:dylanj101

		误报
		http://arenagaming.ir/panel/login:[R]uDe_[B]oy:mahdi99
		http://arenagaming.ir/panel/:[M]eTal_[B]aT:abcc1]
		http://as.fightz.io/:MT_[TR]:159753456258]
		Panmpe_[12]:https://www.roblox.com/my/account:pantelesmpekiares@gmail:com
		http://academico.admision.espol.edu.ec/:infosalavarriaronny@gmail.com:SD_[xWIgURJ]
		https://discord.com/channels/@me:ezyup_[profjust]:Feriteyup1

		// 还是会有少量误报情况，这并没有标准协议，很难界定
		http://gocvuichoi.net/dangnhap.html:darius_[noxus]:Gogeta
		https://discord.com/channels/@me:ezyup_[profjust]:Feriteyup1

		遍历情况
		Application::Microsoft_[Edge]_Profile:1:===============
		Application::Microsoft_[Edge]_Profile:1
		Application::Google_[Chrome]_Default:===============
		Application::Google_[Chrome]_Default
		Microsoft_[Edge]_Default
		Microsoft_[Edge]_Profile
		Google_[Chrome]_Backup
		Google_[Chrome]_Profile:1
		Google_[Chrome]
		Chrome_[Profile_1]
		Yandex_[YandexBrowser]_Default
		CocCoc_[Browser]_Profile:1
		Mail.Ru_[Atom]_Default

		appStrList := []string{"[Edge]", "[Chrome]", "[YandexBrowser]", "[Browser]", "[Atom]", "[Profile_"}
	*/
	// old 2024.2
	// appPattern1 = regexp.MustCompile(`(?i)\b(application: *)?(microsoft|google)_\[(edge|chrome)\]\w+`)
	// appPattern2 = regexp.MustCompile(`(?i)(microsoft|google)_\[(edge|chrome)\]\w+`)
	// old 2024.4.15  部分不规则的误报
	// appPattern = regexp.MustCompile(`(?i)\b(Application:+ *)?([\.a-zA-Z]+)_\[\w+\](_\w+)?(:(\d+|Default))?(:=+)?`)
	appPattern = regexp.MustCompile(`(?i)\b(Application:+ *)?([\.a-zA-Z]{3,})_\[[a-zA-Z]\w+\](_\w+)?(:(\d+|Default))?(:=+)?`)

	// URL标准正则表达式
	urlPattern = regexp.MustCompile(`\b((pop3|javascript|im|ttp|tp|tps|ttps|ht|ftp|http|https|imap|smtp|ssh|telnet|file|content|vivaldi|moz-proxy|moz-extension|oauth|mailbox)://)(localhost|[\-\w][\-\w]{0,62}(\.[\-\w][\-\w]{0,62})+)(:[0-9]{1,5})?(/([\-\w()$@%/!+\.~#?&=]*)|)\b`)

	// 非标准URL正则表达式，hostname不标准
	/*
		http://ccopx11:8085/login|oi064187|gepe9ralpal9dopo
		http://cp4j8wml3:8060/two_factor_auth|admin|admin
		http://homeassistant:8123/auth/authorize|antonio|geronimo
		http://local-dev:8080/signin|IT|qwerty
		http://nas224085:8080|admin|123
		http://localhoust:8080/signin|IT|qwerty
	*/
	nonStandardHostUrlPattern = regexp.MustCompile(`{?i}\b((http)://)([\w\-]*)(:[0-9]{1,5})?(/([\-\w()$@%/!+\.~#?&=]*)|)`)

	// file协议
	fileUrlPattern = regexp.MustCompile(`{?i}\b((file):///)[\w\-]*:(/([\-\w()$@%/!+\.~#?&=]*)|)`)

	// // 浏览器协议正则表达式
	/*
		himmidj@gmail.com:75$Germanyhimmi$75:moz-extension://56449866-0d06-430e-bab6-5aba4b9c2ff2
		hp5n5fogllid7wut2nufxn3wpdj3ibbc:carina90:chrome://weave/
		chrome://weave/:hyuchm6eqau4sxkyvd55irr53ks56bf2:mw5nprbz6sqbzmqrd7avvewrwi
		chrome-extension://caljgklbbfbcjjanaijlacgncafpegll/panel.html:info@mimetika.eu:S=K.MRvlAkZm
		vascoamgeraldes@gmail.com:Anabela1:ms-browser-extension://EdgeExtension_DashlaneDashlaneEdgeExtension_ks9qrcqmdm1bm/
		chrome://weave/:pufj6ua7y4bgf4pbt7gwyru5rvrgmnee:jxt4n6jndjmisqh49rejf8rzqa
	*/
	browserExtUrlPattern = regexp.MustCompile(`{?i}(ms-browser|chrome|moz)(\-extension)?://[\-\w()@%;\\+.~#?&//=]*`)

	// Android应用正则表达式1, 辨识度比较高
	/*
	 */
	androidPattern1 = regexp.MustCompile(`{?i}\bandroid:[\w+/\\-]*={0,2}(@|.)[\w\./]*`)

	// // Android应用正则表达式2, android头丢失的情况
	/*
		ndroid://zQxb6hXv1MJiC1Yyotdhi8HP-y7fwPcOLwPOWTwWKpP2VXcbsuIiI4iJ1KV0Dz3LzVsUuKJmYCBIUAxnsPB9FA==@com.facebook.katana/
		oid://pBowWSLvFMHp-Qulwesr2bSrcr9vsiPPhGed3xcXj5ZNJccRGvbP7pPFSqeoQ8NHooDFe29iIWzU_fETWE2UpQ==@net.slideshare.mobile/
	*/
	androidPattern2 = regexp.MustCompile(`\b\w+://[\-\w()%\\+\\.~#?&//=]+==[@\-\w()%\\+\\.~#?&//=]+`)

	//非标准的url协议
	nonStandardProtocalUrlPattern = regexp.MustCompile(`\b\w+://(localhost|[\-\w][\-\w]{0,62}(\.[\-\w][\-\w]{0,62})+)(:[0-9]{1,5})?(/([\-\w()$@%/!+\.~#?&=]*)|)\b`)

	// // 兜底
	// 这个有误报的情况，比如人名：jaroslav.dushniy:0687827301uk   ; 但是也有这种情况：www.facebook.com:08104064388:|DANIEL|BOSS|
	nonStandardUrlLastPattern = regexp.MustCompile(`\b(//)?([\w][\-\w]{0,62}(\.[\w][\-\w]{0,62})+)(:[0-9]{1,5})?(/[\/\-\w()$@%/!+\.~#?&=])*\b`)

	// // 软件标签正则表达式
	// softPattern = regexp.MustCompile(`^{?i}(browser|soft|host):([ \\w])*http`)

	// // 正常URL后缀正则表达式
	// normalUrlSuffix = regexp.MustCompile(`\.(com|cn|edu|net|org)`)
}

// map的查找复杂度为O(1), 列表查找复杂度为O(n)。但是对于量级少的情况，反而比较慢
// func isInMap(m map[string]int, element string) bool {
// 	_, found := m[element]
// 	return found
// }

// 元素在这个list中，返回true，否则返回false
func isInList(list []string, element string) bool {
	for _, item := range list {
		if item == element {
			return true
		}
	}
	return false
}

// 是否包含特定的子串列表中的各个元素， 并获取位置
func isContainsSubChIndex(line string, subChList []string) (bool, int, string) {
	for _, subCh := range subChList {
		index := strings.Index(line, subCh)
		if index > -1 {
			return true, index, subCh
		}
	}
	return false, -1, ""
}

// 是否包含特定的子串列表中的各个元素.  测试下来Contains性能优于Index
func isContainsSubCh(line string, subChList []string) bool {
	for _, subCh := range subChList {
		if strings.Contains(line, subCh) {
			return true
		}
	}
	return false
}

func isHasInvalidChars(line string) bool {
	return isContainsSubCh(line, invalidCharsInUsername)
}

func getSeparatorOtherSmall(sepDict map[string]int, minValue int) (sepFlag bool, separator string, maxSeparatorCt int) {
	sepFlag = false
	separator = ""
	maxSeparatorCt = 0

	// 判断分隔符个数是否满足判断条件
	countGreaterOrEqualOneMinValue := 0

	// 遍历map，统计值大于等于2的元素数量
	for _, value := range sepDict {
		if value >= minValue {
			countGreaterOrEqualOneMinValue++
			if value > maxSeparatorCt {
				maxSeparatorCt = value
			}
		}
	}

	// 检查是否有且仅有一个元素的值大于等于2
	if countGreaterOrEqualOneMinValue == 1 {
		// 再次遍历map，找到值大于等于2的元素
		for key, value := range sepDict {
			if value >= minValue {
				// 返回该元素的键作为分隔符
				sepFlag = true
				separator = key
			}
		}
	}
	return sepFlag, separator, maxSeparatorCt
}

// 分隔符模式： 主要是2个逻辑：
// 1. 只有1个分隔符大于2， 其余分隔符都小于等于1， 则取这个为分隔符。 注意：分割符号":" 在url中可能出现2次
// 2. 只有1个分隔符大于等于1， 其余分隔符都是0 ，如：lapapeleriadecata@gmail.com:jesucristo1028
// 遇到的情况：ipv6, 冒号多，但是如果没有密码，usernmae可能取错， 99.99.3999:e689w6yj.default:imap://imap-mail.outlook.com|steve-worg@live.de]
// 这种情况要注意：  https://accounts.google.com  ong.makhtoutat:tichit123  ， 去掉url之后， 空格虽然有2个>冒号1个，但是空格是连在一起的，且跟着url的，这里分隔符应该是冒号。如果是空格，这里用trim解决，如果是其他的符号会不会有问题？ 分析的异常情况： ：：abcde xxxxxx
func separatorDetectionMode(line string) (sepFlag bool, separator string, maxSeparatorCt int) {
	tmpSepDict := make(map[string]int)

	sepFlag = false
	separator = ""
	maxSeparatorCt = 0

	// 获取分隔符个数
	for _, tmpSep := range separatorList {
		tmpSepDict[tmpSep] = strings.Count(line, tmpSep)
	}

	// 判断分隔符个数是否满足判断条件
	sepFlag, separator, maxSeparatorCt = getSeparatorOtherSmall(tmpSepDict, 2)
	if !sepFlag {
		// 单个有误报情况，比如单行就1个url~~~~~  http://abc.com/serea.html   这种理论上优先级要比正则低，或者正则之后要不全
		sepFlag, separator, maxSeparatorCt = getSeparatorOtherSmall(tmpSepDict, 1)
	}

	return sepFlag, separator, maxSeparatorCt
}

func parseUsername(line string) (username string) {
	username = ""
	if strings.Contains(line, "@") {
		mMatches := mailPattern.FindAllString(line, -1)
		if mMatches != nil {
			username = mMatches[0]
		}
	}
	return username
}

func parseApp(line string) (app string) {
	app = ""
	if strings.Contains(line, "_[") {
		appMatches := appPattern.FindStringSubmatch(line)
		if appMatches != nil {
			app = appMatches[0]
		}
	}
	for _, appTag := range appStrList {
		if strings.Contains(line, appTag) {
			return app
		}
	}
	return ""
}

func parseUrl(line string) (url, standardUrl, otherUrl, nonStandardUrl string) {

	var u_match []string
	url = ""
	standardUrl = ""
	otherUrl = ""
	nonStandardUrl = ""

	lowerLine := strings.ToLower(line)

	// url parse
	if strings.Contains(line, "://") && urlPattern.FindAllString(line, -1) != nil {
		u_match = urlPattern.FindAllString(line, -1)
		standardUrl = u_match[0]
		url = standardUrl
	} else if strings.Contains(lowerLine, "android") && androidPattern1.FindAllString(line, -1) != nil {
		otherUrl = androidPattern1.FindAllString(line, -1)[0]
		url = otherUrl
	} else if strings.Contains(lowerLine, "http") && nonStandardHostUrlPattern.FindAllString(line, -1) != nil {
		// http开头的情况
		otherUrl = nonStandardHostUrlPattern.FindAllString(line, -1)[0]
		url = otherUrl
	} else if strings.Contains(lowerLine, "file") && fileUrlPattern.FindAllString(line, -1) != nil {
		otherUrl = fileUrlPattern.FindAllString(line, -1)[0]
		url = otherUrl
	} else if strings.Contains(lowerLine, "//") && browserExtUrlPattern.FindAllString(line, -1) != nil {
		otherUrl = browserExtUrlPattern.FindAllString(line, -1)[0]
		url = otherUrl
	} else if androidPattern2.FindAllString(line, -1) != nil {
		// 大部分误报的情况，不过啥也不是
		otherUrl = androidPattern2.FindAllString(line, -1)[0]
		url = otherUrl
	} else if strings.Contains(line, "://") && nonStandardProtocalUrlPattern.FindAllString(line, -1) != nil {
		otherUrl = nonStandardProtocalUrlPattern.FindAllString(line, -1)[0]
		url = otherUrl
	} else if nonStandardUrlLastPattern.FindAllString(line, -1) != nil {
		nonStandardUrl = nonStandardUrlLastPattern.FindAllString(line, -1)[0]
		url = nonStandardUrl
	}

	logger.Tracef("[parseUrl] url: %s, standardUrl: %s, otherUrl: %s, nonStandardUrl: %s", url, standardUrl, otherUrl, nonStandardUrl)
	return url, standardUrl, otherUrl, nonStandardUrl
}

// 计算逻辑比较复杂，会比较消耗性能
func parseOneLine(line string) (parsedData SgkBrowserDataRecord) {

	sepFlag := false
	separator := ""
	username := ""
	password := ""
	app := ""
	url := ""
	standardUrl := ""
	otherUrl := ""
	nonStandardUrl := ""
	maxSeparatorCt := 0
	// startTm := time.Now()
	parsedData = SgkBrowserDataRecord{}

	// debug
	// return parsedData

	// 如果url超长，直接返回，不做处理, 这种情况很少，判断影响性能
	// lineLen := len(line)
	// if lineLen > 8192 {
	// 	logger.Warnf("[parsedLine][Warn]lines too long[%d]: %s", lineLen, line)
	// 	return parsedData
	// }

	// 外部使用了goroutinue， 整体得到了优化，内部如果再使用goroutinue，虽然单个运行的时间降低，但整体耗时增加了（有太多资源消耗在传递上），这里面应该尽量优化内部的逻辑，而不是通过并发优化
	// ------正则匹配，按照顺序（优先级）
	//  原则：先字符判断，再正则，提高性能

	app = parseApp(line)
	// 任务完成，取值. 未完成，会阻塞. 在需要使用的地方取值，对于耗时较多的任务，放到后面
	// 重点：这里会做line 级别的修订
	if app != "" {
		// app 不为空，line去掉app字符，再进行处理, 更准确
		// logger.Tracef("\n\t\t\tapp[%s] line[%s]", app, line)
		line = strings.ReplaceAll(line, app, "")
	}

	// urlChan := make(chan string)
	// usernameChan := make(chan string)
	// 提取url， 正则最多，最耗时间，放到前面  10-20µs
	// go func(line string) {
	// 	url, standardUrl, otherUrl, nonStandardUrl := parseUrl(line)
	// 	urlChan <- url
	// 	urlChan <- standardUrl
	// 	urlChan <- otherUrl
	// 	urlChan <- nonStandardUrl
	// 	close(urlChan)
	// }(line)
	url, standardUrl, otherUrl, nonStandardUrl = parseUrl(line)

	// 提取username[mail] parse, 非常普遍，耗时也相对较多 5-10us,  500ns
	// go func(line string) {
	// 	username := parseUsername(line)
	// 	usernameChan <- username
	// 	close(usernameChan)
	// }(line)
	username = parseUsername(line)

	// 分隔符模式的逻辑判断
	sepFlag, separator, maxSeparatorCt = separatorDetectionMode(line)

	// username = <-usernameChan
	// url = <-urlChan
	// standardUrl = <-urlChan
	// otherUrl = <-urlChan
	// nonStandardUrl = <-urlChan

	// url和username修订逻辑
	// 异常1 url是邮箱后缀，取值错误，清空url
	if nonStandardUrl != "" && strings.Contains(username, nonStandardUrl) {
		nonStandardUrl = ""
		url = ""
	}
	// 异常2 邮箱在标准url中，取值错误。 username并不是邮箱地址
	if username != "" && (strings.Contains(standardUrl, username) || strings.Contains(otherUrl, username)) {
		username = ""
	}

	// 二次尝试
	if !sepFlag {
		// 如果整行获取分隔符失败，尝试去掉url再进行匹配
		// 比如： https://e-compras.curitiba.pr.gov.br/Default.aspx:By Jani Mendonça:Mendonca102303
		// https://www.warframe.com/pt-br/signup|Setrick|z7H4e-qi:n5tx4B
		if url != "" {
			noUrlStr := strings.TrimSpace(strings.ReplaceAll(line, url, ""))
			sepFlag, separator, maxSeparatorCt = separatorDetectionMode(noUrlStr)
		}
	}

	if sepFlag {
		// 成功获取分隔符，开始进行分割处理，提取 username+password 作为upParts，后续再处理
		// logger.Tracef("\t[SeparatorMode] sep[%s] line:%s", separator, line)

		var upParts []string

		if url == "" {
			// url 为空的情况
			// 确实没有url的情况(包括非标准url和其他app情况)
			upParts = strings.Split(line, separator)
		} else if nonStandardUrl != "" && maxSeparatorCt == 1 {
			// 如果分隔符只有1个，则该非标准url算作误判， 且置空
			upParts = strings.Split(line, separator)
			url = ""
		} else {
			// url 不为空的情况

			// url在最前面，这种情况最多
			if strings.HasPrefix(line, url) {
				tmpUpParts := strings.Replace(line, url, "", len(url))
				upParts = strings.Split(tmpUpParts, separator)
				// logger.Tracef("\t[SeparatorMode] tmpUpParts:%s, upParts:%v", tmpUpParts, upParts)
				if string(upParts[0]) != "" {
					// 【url补全】
					// url匹配不完全正确，补全url，取username和password . 这种情况比较少，主要是非标准url的情况会有，要注意url，和后面可能是其他分隔符，这里要做一层判断, 如果不包含分隔符，则补全，否则不进行补全。
					// 注意，存在url和username分隔符  与 username和password分隔符不同的情况，如果不同，要单独对待： https://www.linkedin.com/signup/cold-join cjdverde@gmail.com:Wincar8!
					// index, _, isContainsFlag := isContainsSeparator(upParts[0])
					isContainsFlag, index, _ := isContainsSubChIndex(upParts[0], separatorList)
					if !isContainsFlag {
						newUrl := url + upParts[0]
						url = newUrl
						upParts = upParts[1:]
					} else {
						// 包含分隔符，url更新到新分隔符位置，upParts[0] 更新
						newUrl := url + upParts[0][:index]
						url = newUrl
						upParts[0] = upParts[0][index+1:]
					}
				}

			} else if strings.HasSuffix(line, url) {
				// url 在最后面
				tmpUpParts := strings.Replace(line, url, "", len(url))
				upParts = strings.Split(tmpUpParts, separator)
				upParts = upParts[:len(upParts)-1]
			} else {
				// url 在中间，可能是前面，也可能是后面
				prePartsStr := line[:strings.Index(line, url)]
				sufPartsStr := line[strings.Index(line, url)+len(url):]
				preParts := strings.Split(prePartsStr, separator)
				sufParts := strings.Split(sufPartsStr, separator)

				if preParts[len(preParts)-1] != "" {
					// 说明url前面不是分隔符，去除非分隔符字符串
					preParts = preParts[:len(preParts)-1]
				}

				if sufParts[0] != "" {
					// 【url补全】
					// 说明url后面不是分隔符，自动补全url,这里如果url后面包含其他分隔符，则不补全到其他分隔符为止
					// index, _, isContainsFlag := isContainsSeparator(sufParts[0])
					isContainsFlag, index, _ := isContainsSubChIndex(sufParts[0], separatorList)
					if !isContainsFlag {
						newUrl := url + sufParts[0]
						url = newUrl
						sufParts = sufParts[1:]
					} else {
						// 包含分隔符，url更新到新分隔符位置，upParts[0] 更新
						newUrl := url + sufParts[0][:index]
						url = newUrl
						sufParts[0] = sufParts[0][index+1:]
					}

				}
				upParts = append(preParts, sufParts...)
			}
		}

		newUpParts := []string{}
		for _, item := range upParts {
			if item != "" {
				newUpParts = append(newUpParts, item)
			}
		}
		upParts = newUpParts

		// 开始分割upParts，提取username和password
		if len(upParts) > 0 {
			if username != "" {
				// username不为空

				// 从upParts 提取用户名和密码
				if strings.Contains(upParts[0], username) {
					// username在最开始，这种情况最多
					if upParts[0] != username {
						// 补全username
						username = upParts[0]
					}
					password = strings.Join(upParts[1:], separator)
				} else if strings.Contains(upParts[len(upParts)-1], username) {
					// username在最后面
					if upParts[len(upParts)-1] != username {
						// 补全username
						username = upParts[len(upParts)-1]
					}
					password = strings.Join(upParts[:len(upParts)-1], separator)
				} else {
					// username在中间, 密码就是去掉username字符串拼接
					// 注意，存在分隔符，但是也不是以username开始的情况： line www.facebook.com:(www.facebook.com/login.php):midotito326@yahoo.com:uwk-mohamedsherry  ->   (/login.php) midotito326@yahoo.com uwk-mohamedsherry
					for i, pt := range upParts {
						if strings.Contains(pt, username) {
							if pt != username {
								// 补全username
								// fmt.Printf("-------1[fix username]middle| old[%s] new[%s] sep[%s] line[%s]\n", username, pt, separator, line)
								username = pt
							}

							if len(upParts[i+1]) >= 6 {
								// usernmae后面有字符，且长度大于6，则认为是密码
								password = upParts[i+1]
							} else {
								// 否则，把所有字符拼接记录
								password = strings.Join(append(upParts[:i], upParts[i+1:]...), separator)
								// logger.Debug("[uparts]", upParts, "i:", i, "pt:", password)
							}
							break
						}
					}
				}
			} else {
				// username为空(非邮箱格式)
				//  以 username 分隔符 password 格式解析, 这里情况比较多，比如username为空，或者超过2个元素，第一个元素并不是url的情况，做分别处理
				// logger.Tracef("\t[Separator][No username]up[%s] line: %s", upParts, line)

				lenParts := len(upParts)
				if lenParts == 1 {
					// 只有1个，默认是username
					username = upParts[0]
				} else if lenParts == 2 {
					username = upParts[0]
					password = upParts[1]
				} else {
					for i, up := range upParts {
						if !isHasInvalidChars(up) {
							username = up
							password = strings.Join(upParts[i+1:], separator)
							// logger.Tracef("\t[Separator][No username][multipart]sep[%s] i[%d] u[%s] p[%s] up[%s] line: %s", separator, i, username, password, upParts, line)
							break
						}
					}

				}
			}
		}
	} else {
		// 获取分隔符失败。 这里的逻辑就会比较复杂，主要是通过url和username来判断
		logger.Tracef("\t[NoSeparatorMode]%s", line)
		// 先根据url，username等正则判断
		if username != "" {
			// 分隔符失败——> username获取成功，根据username位置获取分隔符
			// logger.Tracef("\t[NoSeparatorMode] -> username[%s]", username)
			/*
				https://www.primecursos.com.br/curriculo/|Baslio Jos Augusto Jos|basiliio.jose@gmail.com web
				https://canvas.instructure.com/|mdabdulfahad41@gmail.com|MD abdul fahad web
				 https://mail.google.com/mail/u/0/#inbox/FMfcgzGlkPSlZbZNMBpwlLggJpzBRBSM|[Unipordenone] Dettagli Login - marco.vernier@gmail.com - Gmail|GREEN PASS obbligo per tutti i lavoratori dal 15 ottobre 2021 - marco.vernier@gmail.com - Gmail web
			*/
			// 有url
			if url != "" {
				// 分隔符失败——> username 获取成功 -> url获取成功
				// logger.Tracef("\t[NoSeparatorMode] -> username[%s] -> url[%s] ", username, url)
				beforePartsStr := line[:strings.Index(line, username)]
				afterPartsStr := line[strings.Index(line, username)+len(username):]

				if strings.Index(line, username) > strings.Index(line, url) {
					// url 在 username 前面，取url取后面部分（这里不能取username开始，因为username可能在最后）
					// upPartsStr = line[strings.Index(line, url)+len(url):]
					// url 在username前面，从username往前面遍历
					// url和username分隔符不同的情况怎么处理？  暂时不处理

					for i := len(beforePartsStr) - 1; i >= 0; i-- {
						if isInList(separatorList, string(beforePartsStr[i])) {
							// if isInMap(separatorMap, string(beforePartsStr[i])) {
							separator = string(beforePartsStr[i])
							// fmt.Println("[Username Traversal OK]sep->", separator, "before:", beforePartsStr, "username:", username, "url:", url, "line:", line)
							break
						}
					}

					if separator == "" {
						// fmt.Println("[Username Traversal failed]before:", beforePartsStr, "username:", username, "line:", line, app)
					} else {
						// fix username
						beforeParts := strings.Split(beforePartsStr, separator)
						afterParts := strings.Split(afterPartsStr, separator)
						newUsername := beforeParts[len(beforeParts)-1] + username + afterParts[0]
						// fmt.Printf("------------u[%s] newu[%s] bp[%s] ap[%s]\n", username, newUsername, beforeParts, afterParts)
						username = newUsername

						// fix url
						urlafterPartsStr := line[strings.Index(line, url)+len(url):]
						urlafterParts := strings.Split(urlafterPartsStr, separator)
						if urlafterParts[0] != "" {
							newUrl := url + urlafterParts[0]
							url = newUrl
						}

						// get password. 要不在url和username中间，要么在username后面

						if afterPartsStr != "" {
							// 默认情况password在url后面
							password = strings.Join(afterParts[1:], separator)
						} else if len(urlafterParts) > 1 && username != urlafterParts[1] {
							// username不在url后面，password取之间的内容
							password = strings.Join(strings.Split(urlafterPartsStr[:strings.Index(urlafterPartsStr, username)], separator)[1:], separator)
						}

						// fmt.Println("[Username Traversal]sep->", separator, "before:", beforePartsStr, "username:", username, "password:", password, "url:", url, "line:", line, app)
					}

				} else if strings.Index(line, url) > strings.Index(line, username) {
					// url 在 username 后面，去掉url取前面部分，从username后面遍历分隔符
					// upPartsStr = line[strings.Index(line, username):strings.Index(line, url)]

					lastPartsStr := line[len(username):]
					for _, ch := range lastPartsStr {
						if isInList(separatorList, string(ch)) {
							// if isInMap(separatorMap, string(ch)) {
							separator = string(ch)
							// fmt.Println("[Username Traversal OK]sep->", separator, "after:", lastPartsStr, "username:", username, "url:", url, "line:", line)
							break
						}
					}

					if separator == "" {
						// fmt.Println("[Username Traversal failed]after:", lastPartsStr, "username:", username, "line:", line)
					} else {
						// fix username
						beforeParts := strings.Split(beforePartsStr, separator)
						afterParts := strings.Split(afterPartsStr, separator)
						newUsername := beforeParts[len(beforeParts)-1] + username + afterParts[0]
						// fmt.Printf("------------u[%s] newu[%s] bp[%s] ap[%s]\n", username, newUsername, beforeParts, afterParts)
						username = newUsername

						// fix url
						urlafterPartsStr := line[strings.Index(line, url)+len(url):]
						urlafterParts := strings.Split(urlafterPartsStr, separator)
						if urlafterParts[0] != "" {
							newUrl := url + urlafterParts[0]
							url = newUrl
						}

						// get password. 大概率在username后面，小概率在username和url后面，或者在username前面
						if len(afterParts) > 1 {
							if url != afterParts[1] {
								// 如果url不直接排在username后面, 密码就取username后面+url前面
								if strings.Index(afterPartsStr, url) > 0 {
									tmpParts := strings.Split(afterPartsStr[:strings.Index(afterPartsStr, url)], separator)
									if len(tmpParts) > 1 {
										password = strings.Join(tmpParts[1:], separator)
									}
								}
							}
						} else {
							// username后面就是url，密码就取username前面+url后面
							password = strings.Join(beforeParts[:len(beforeParts)-1], separator) + strings.Join(urlafterParts[1:], separator)

							// fmt.Println("------------------------[Username Traversal]sep->", separator, "after:", lastPartsStr, "username:", username, "password:", password, "url:", url, "line:", line)

						}
						// fmt.Println("[Username Traversal]sep->", separator, "before:", beforePartsStr, "username:", username, "password:", password, "url:", url, "line:", line, app)

					}

				}

			} else {
				// logger.Tracef("\t[NoSeparatorMode] -> username[%s] -> [no url] ", username)
				// 分隔符失败——> username 获取成功 -> url获取失败
				// url 获取失败的情况。
				// 这种情况大部分都是不正常的，分隔符也没获取到，url也没获取到（包括非标准），投入较少精力处理
				// 少部分情况，分隔符重复： //accounts.google.com:|:gilson2lopes@gmail.com:|:Sa03252804#:|:https

				beforePartsStr := line[:strings.Index(line, username)]
				afterPartsStr := line[strings.Index(line, username)+len(username):]

				if beforePartsStr != "" {
					// username前面有字符, 反向遍历分隔符
					for i := len(beforePartsStr) - 1; i >= 0; i-- {
						if isInList(separatorList, string(beforePartsStr[i])) {
							// if isInMap(separatorMap, string(beforePartsStr[i])) {
							separator = string(beforePartsStr[i])
							// fmt.Println("[Username Traversal OK]sep->", separator, "before:", beforePartsStr, "username:", username, "url:", url, "line:", line)
							break
						}
					}
				}
				if separator == "" {
					// 前面没有遍历到，尝试遍历后面
					if afterPartsStr != "" {
						for _, ch := range afterPartsStr {
							if isInList(separatorList, string(ch)) {
								// if isInMap(separatorMap, string(ch)) {
								separator = string(ch)
								// fmt.Println("[Username Traversal OK]sep->", separator, "after:", lastPartsStr, "username:", username, "url:", url, "line:", line)
								break
							}
						}
					}
				}

				if separator != "" {
					// 分割遍历
					beforeParts := strings.Split(beforePartsStr, separator)
					afterParts := strings.Split(afterPartsStr, separator)
					newUsername := beforeParts[len(beforeParts)-1] + username + afterParts[0]
					username = newUsername

					// 默认密码为后面的字符
					if afterPartsStr != "" && len(afterParts) > 1 {
						if afterParts[1] != "" {
							password = strings.Join(afterParts[1:], separator)
						}
					} else if beforePartsStr != "" && len(beforeParts) > 1 {
						if beforeParts[len(beforeParts)-1] != "" {
							password = strings.Join(beforeParts[:len(beforeParts)-1], separator)
						}
					}
				}

			}

		} else {
			//分隔符失败——> username 获取失败
			// logger.Tracef("\t[NoSeparatorMode] -> [no username] %s", line)

			//  username获取失败
			if url != "" {
				// logger.Tracef("\t[NoSeparatorMode] -> [no username] -> url[%s] ", url)
				//分隔符失败——> username 获取失败 -> url获取成功

				if strings.Index(line, url) == -1 {
					logger.Warnf("[url Traversal][Error]Can't Find url in line.....sep[%s] u[%s] p[%s] url[%s], line: %s", separator, username, password, url, line)
				}
				beforePartsStr := line[:strings.Index(line, url)]
				afterPartsStr := line[strings.Index(line, url)+len(url):]

				if beforePartsStr != "" {
					// url前面有字符, 反向遍历分隔符
					for i := len(beforePartsStr) - 1; i >= 0; i-- {
						if isInList(separatorList, string(beforePartsStr[i])) {
							separator = string(beforePartsStr[i])
							// fmt.Println("[url Traversal OK]sep->", separator, "before:", beforePartsStr, "username:", username, "url:", url, "line:", line)
							break
						}
					}
				}

				if separator == "" {
					// 前面没有遍历到，尝试遍历后面
					if afterPartsStr != "" {
						for _, ch := range afterPartsStr {
							if isInList(separatorList, string(ch)) {
								separator = string(ch)
								// fmt.Println("[url Traversal OK]sep->", separator, "after:", lastPartsStr, "username:", username, "url:", url, "line:", line)
								break
							}
						}
					}
				}

				if separator != "" {
					// 分割遍历
					beforeParts := strings.Split(beforePartsStr, separator)
					afterParts := strings.Split(afterPartsStr, separator)
					newUsername := beforeParts[len(beforeParts)-1] + username + afterParts[0]
					username = newUsername

					// 默认格式：url, username, password

					if len(afterParts) == 2 {
						username = afterParts[1]
					} else if len(afterParts) > 2 {
						username = afterParts[1]
						password = strings.Join(afterParts[2:], separator)
					} else {
						// 后面没有，则从前面开始， username, password, url
						if len(beforeParts) == 2 {
							username = beforeParts[0]
						} else if len(beforeParts) > 2 {
							username = beforeParts[0]
							password = strings.Join(beforeParts[1:len(beforeParts)-1], separator)
						}
					}
				}
			}
			// } else {
			// 	// 获取username、和url都失败。 获取sep也失败
			// 	// fmt.Println("[No sep][no username][no url]line:", line)
			// }
		}
	}

	usernameLen := len(username)
	passwordLen := len(password)

	// 去除首位空格，防止空格分隔符识别导致包含空格
	parsedData.Username = strings.TrimSpace(username)
	if usernameLen > 100 || usernameLen < 5 {
		// logger.Tracef("\t[Check Username]u[%s] p[%s] url[%s], line:%s", username, password, url, line)
	}
	if passwordLen > 512 {
		parsedData.Password = "[TooLong]" + password[:10]
		// logger.Tracef("\t[Check Password]u[%s] p[%s] url[%s], line:%s", username, password, url, line)
	} else if passwordLen < 5 {
		// logger.Tracef("\t[Check Password]u[%s] p[%s] url[%s], line:%s", username, password, url, line)
	} else {
		parsedData.Password = strings.TrimSpace(password)
	}

	if url == "" && app != "" {
		// url为空，将app作为url记录，方便排查问题
		url = app
	}

	if len(url) > 1024 {
		parsedData.URL = strings.TrimSpace(url)[:512] + "[TooLong]"
		// logger.Tracef("\t[Check Url]u[%s] p[%s] url[%s], line:%s", username, password, url, line)
	} else {
		parsedData.URL = strings.TrimSpace(url)
	}
	parsedData.Separator = separator

	// logger.Trace("------oneline time:", time.Since(startTm))

	return parsedData
}

func processLines(linesChan chan LinesFileInfo, insertdbChan chan DbDataRecordList) {

	for linesInfo := range linesChan {
		if linesInfo.LinesList == nil {
			break
		}

		okCt := 0
		errCt := 0
		repeatCt := 0
		lineCt := len(linesInfo.LinesList)
		tmp_dict := make(map[string]bool)

		startTime := time.Now()
		var parsedDataList DbDataRecordList
		parsedDataList = DbDataRecordList{}

		pdCh := make(chan SgkBrowserDataRecord, lineCt)

		var wg sync.WaitGroup
		// 控制并发，
		// maxQueryCount := 1000
		// guard := make(chan struct{}, maxQueryCount)

		for _, line := range linesInfo.LinesList {
			// 这里不用，在读取的时候就要处理掉空格
			// line = strings.TrimSpace(line)
			if line != "" {
				if _, ok := tmp_dict[line]; ok {
					repeatCt++
					continue // 跳过重复行
				}

				tmp_dict[line] = true
				wg.Add(1)
				// guard <- struct{}{}
				go func(line string) {
					defer func() {
						// <-guard
						wg.Done()
					}()
					pdCh <- parseOneLine(line)
				}(line)
			}
		}

		wg.Wait()
		close(pdCh)

		for parsedData := range pdCh {
			tmpDbDataRecord := DbDataRecord{}
			if parsedData.Separator == "" {
				errCt++
			} else {
				tmpDbDataRecord.Username = parsedData.Username
				tmpDbDataRecord.Password = parsedData.Password
				tmpDbDataRecord.URL = parsedData.URL
				tmpDbDataRecord.Source = linesInfo.FileName
				tmpDbDataRecord.SourceDate = linesInfo.UpdateDate

				// parsedDataCh <- tmpDbDataRecord
				okCt++
				parsedDataList = append(parsedDataList, tmpDbDataRecord)
			}
		}

		if parsedDataList != nil {
			insertdbChan <- parsedDataList
		}

		// 处理完成，清理内存
		linesInfo.LinesList = nil
		linesInfo = LinesFileInfo{}
		tmp_dict = nil
		parsedDataList = nil

		logger.Infof("[ProcessLines]length:%d okCt:%d repeatCt: %d  errCt:%d time:%s ChanLen: %d | %d \n", lineCt, okCt, repeatCt, errCt, time.Since(startTime), len(linesChan), len(insertdbChan))
	}
}

// ////

func getlastIDFromDB(client *sql.DB, table string) (int64, error) {
	lastID := int64(0)

	// Get the last ID
	err := client.QueryRow(fmt.Sprintf("SELECT id FROM %s ORDER BY id DESC LIMIT 1", table)).Scan(&lastID)
	if err != nil {
		return lastID, err
	}
	return lastID, nil
}

func insertToDBDep(client *sql.DB, table string, resultCh chan DbDataRecordList, strictMode bool) {
	nid := int64(0)
	wg := sync.WaitGroup{}

	lastID, err := getlastIDFromDB(client, table)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			nid = 0
		} else {
			logger.Error("[Insert]", err)
		}
	}

	logger.Info("[Insert]Last id:", lastID)

	nid = lastID
	for item := range resultCh {
		if item == nil {
			break
		}
		startTime := time.Now()

		tx, err := client.Begin()
		if err != nil {
			logger.Warnf("[Insert]Error starting insertToDB: %v", err)
		}

		stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (id, username, password, url,source, sourcedate) VALUES (?, ?, ?, ?, ?, ?)", table))
		if err != nil {
			logger.Warnf("[Insert]Error preparing  data: %v", err)
		}

		repeatCt := 0
		for _, row := range item {

			// 严格模式，不记录不规范的情况，不规范情况如下：
			/*
				查询发现，这类占比不多
				长度要求：
					1. 用户名长度小于等于3位（考虑到test,admin,sa等情况），必须密码也小于等于3位（root, root)，不记录到数据库： (会漏掉sa,sa这种情况，在web中不考虑这种情况)

			*/
			// if strictMode {
			// 	usernameLen := len(row.Username)
			// 	passwordLen := len(row.Password)
			// 	if usernameLen <= 3 && passwordLen <= 3 {
			// 		continue
			// 	}
			// }

			// 通过bloomfilter过滤器判断重复
			bfStr := fmt.Sprintf("%s_%s_%s", row.Username, row.Password, row.URL)
			if !bloomFilter.TestString(bfStr) {
				nid++
				_, err := stmt.Exec(nid, row.Username, row.Password, row.URL, row.Source, row.SourceDate)
				if err != nil {
					logger.Warnf("[Insert]Error inserting data: %v | %v", err, row)
					tx.Rollback()
					break
				}

				wg.Add(1)
				go func(bfStr string) {
					defer wg.Done()
					bloomFilter.AddString(bfStr)
				}(bfStr)

			} else {
				repeatCt++
			}
		}

		if err := tx.Commit(); err != nil {
			logger.Warnf("[Insert]Error committing data: %v", err)
			tx.Rollback()
		} else {
			// 只有在提交成功后才关闭 stmt
			stmt.Close()
		}

		wg.Wait()

		logger.Debugf("[Insert]done. rate[%.2f%%]=repeatCt[%d]/rawData[%d], ChanLen[%d], time[%v]", float64(repeatCt)/float64(len(item))*100, repeatCt, len(item), len(resultCh), time.Since(startTime))
	}

}

func insertToDB(client *sql.DB, table string, resultCh chan DbDataRecordList, strictMode bool) {
	nid := int64(0)

	lastID, err := getlastIDFromDB(client, table)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			nid = 0
		} else {
			logger.Error("[Insert]", err)
		}
	}

	logger.Info("[Insert]Last id:", lastID)

	nid = lastID
	for item := range resultCh {
		if item == nil {
			break
		}
		startTime := time.Now()

		tx, err := client.Begin()
		if err != nil {
			logger.Warnf("[Insert]Error starting insertToDB: %v", err)
		}

		stmt, err := tx.Prepare(fmt.Sprintf("INSERT INTO %s (id, username, password, url,source, sourcedate) VALUES (?, ?, ?, ?, ?, ?)", table))
		if err != nil {
			logger.Warnf("[Insert]Error preparing  data: %v", err)
		}

		for _, row := range item {
			nid++

			// 严格模式，不记录不规范的情况，不规范情况如下：
			/*
				查询发现，这类占比不多
				长度要求：
					1. 用户名长度小于等于3位（考虑到test,admin,sa等情况），必须密码也小于等于3位（root, root)，不记录到数据库： (会漏掉sa,sa这种情况，在web中不考虑这种情况)

			*/
			// if strictMode {
			// 	usernameLen := len(row.Username)
			// 	passwordLen := len(row.Password)
			// 	if usernameLen <= 3 && passwordLen <= 3 {
			// 		continue
			// 	}
			// }

			_, err := stmt.Exec(nid, row.Username, row.Password, row.URL, row.Source, row.SourceDate)
			if err != nil {
				logger.Warnf("[Insert]Error inserting data: %v", err)
				tx.Rollback()
				break
			}
		}

		if err := tx.Commit(); err != nil {
			logger.Warnf("[Insert]Error committing data: %v", err)
			tx.Rollback()
		} else {
			// 只有在提交成功后才关闭 stmt
			stmt.Close()
		}

		logger.Debugf("[Insert]done. %d, ChanLen: %d, time: %v len", len(item), len(resultCh), time.Since(startTime))
	}

}

// 这里不要做一些整个文件记录的事情，比如不能在读文件时去重复，如果是大文件，map太大内存会吃光
func readLines(filePath string, updateDate string, linesChan chan LinesFileInfo) {

	linesCt := 0
	handleCt := 0
	linesChunkCt := 0
	longErrCt := 0

	startTime := time.Now()

	var linesList []string
	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		logger.Errorf("Error opening file: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		logger.Infof("[readLines]%s | file Stat error: %s\n", filePath, err)
	} else {
		logger.Infof("[readLines]%s | File size: %.2f M\n", filePath, float64(fi.Size())/1024/1024)
	}

	reader := bufio.NewReader(file)
	var linesInfo LinesFileInfo

	for {

		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break // 文件结束
			}
			logger.Errorf("Error reading line: %v", err)
			break // 遇到错误，退出循环
		}

		linesCt++
		if isPrefix {
			longErrCt++
			continue // 跳过超长行
		}

		lineStr := strings.TrimSpace(string(line))
		line = nil
		if lineStr == "" {
			continue // 跳过空行
		}

		handleCt++
		linesChunkCt++

		linesList = append(linesList, lineStr)

		if linesChunkCt >= linesChunk {
			linesInfo.FileName = filepath.Base(filePath)
			linesInfo.UpdateDate = updateDate
			linesInfo.LinesList = linesList
			linesChan <- linesInfo
			linesChunkCt = 0
			linesList = nil
		}
	}

	if linesChunkCt > 0 {
		linesInfo.FileName = filepath.Base(filePath)
		linesInfo.UpdateDate = updateDate
		linesInfo.LinesList = linesList
		linesChan <- linesInfo
		linesChunkCt = 0
		linesList = nil
	}

	logger.Info("......", len(linesInfo.LinesList), len(linesChan))
	// 清理内存
	linesInfo = LinesFileInfo{}

	elapsedTime := time.Since(startTime)

	logger.Infof("[readLines]Over:%s ParseRate[%.2f M/S] = tm[%.2fs] / Size:[%.2fM] linesCt:%d handleCt: %d  longErrCt: %d  linesChanLen: %d  \n", filePath, float64(fi.Size())/1024/1024/elapsedTime.Seconds(), elapsedTime.Seconds(), float64(fi.Size())/1024/1024, linesCt, handleCt, longErrCt, len(linesChan))
}

func readDir(dirPath string, updateDate string, linesChan chan LinesFileInfo) {

	logger.Infof("[readDir]Start... Dir: %s", dirPath)

	filepath.Walk(dirPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			logger.Info("[readDir]", err)
			return err
		}
		// 检查文件扩展名是否为.txt
		if filepath.Ext(path) == ".txt" {
			readLines(path, updateDate, linesChan)
		}
		// 继续遍历
		return nil
	})

}

// ////////////////main////
func processDirMain(table string, dirPath string, updateDateStr string) {

	var wg sync.WaitGroup
	startTm := time.Now()

	insertDbIsStrict := true

	logger.Infof("[processDirMain]Starting parse & import data. Table[%s] | Dir[%s] | UpdateDate[%s]", table, dirPath, updateDateStr)
	client, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000?database=sgk")
	if err != nil {
		logger.Error(err)
	}
	defer client.Close()

	linesChan := make(chan LinesFileInfo, 3)
	insertdbChan := make(chan DbDataRecordList, 2)

	wg.Add(1)
	go func(dirPath string, updateDateStr string) {
		defer wg.Done()
		logger.Info("[readLines]goroutine start....")
		readDir(dirPath, updateDateStr, linesChan)
		logger.Info("[readLines]goroutine Over....")

		linesChan <- LinesFileInfo{LinesList: nil}
	}(dirPath, updateDateStr)

	wg.Add(1)
	go func() {
		defer wg.Done()
		logger.Info("[processLines]goroutine start....")
		processLines(linesChan, insertdbChan)
		logger.Info("[processLines Over]....")

		insertdbChan <- nil

	}()

	// Start the database insertion goroutine
	wg.Add(1)
	go func(client *sql.DB, table string, insertDbIsStrict bool) {
		defer wg.Done()
		logger.Info("[insertFunc]goroutine start....")
		// insertToDB(client, table, insertdbChan, insertDbIsStrict)
		insertToDBDep(client, table, insertdbChan, insertDbIsStrict)
		logger.Info("[Insert Over]....")
	}(client, table, insertDbIsStrict)

	wg.Wait()
	close(linesChan)
	close(insertdbChan)

	elapsedHours := time.Since(startTm).Minutes()
	logger.Infof("[Main Task Over] ...[Elapsed] %.2fm", elapsedHours)
}

// debug ....
func fileDebug() {
	logger.SetLevel(logrus.DebugLevel)
	logger.Debug("file Debug.....")

	startTime := time.Now()
	linesCt := 0
	longErrCt := 0

	var filePath string
	filePath = "/mnt/e/Link_Login_Pass ( FREE CLOUD LOGS )/2024_03tmp/130kk_URL_LOG_PASS.txt"

	// 打开文件
	file, err := os.Open(filePath)
	if err != nil {
		logger.Errorf("Error opening file: %v", err)
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		logger.Warnf("[readLines]%s | file Stat error: %s\n", filePath, err)
	} else {
		logger.Infof("[readLines]%s | File size: %.2f M\n", filePath, float64(fi.Size())/1024/1024)
	}

	reader := bufio.NewReader(file)

	for {
		line, isPrefix, err := reader.ReadLine()
		if err != nil {
			if err == io.EOF {
				break // 文件结束
			}
			fmt.Printf("Error reading line: %v", err)
			break // 遇到错误，退出循环
		}

		linesCt++
		if isPrefix {
			longErrCt++
			continue // 跳过超长行
		}

		lineStr := strings.TrimSpace(string(line))
		if lineStr == "" {
			continue // 跳过空行
		}

		parsedData := parseOneLine(lineStr)
		logger.Tracef("\t[Parsed]sep[%s] user[%s] pass[%s] url[%s] %s\n",
			parsedData.Separator,
			parsedData.Username,
			parsedData.Password,
			parsedData.URL,
			line)

	}

	elapsedTime := time.Since(startTime)

	logger.Debugf("[file debug]Over:%s ParseRate[%.2f M/S] = tm[%.2f] / Size:[%.2fM] linesCt:%d longErrCt: %d   chan: \n", filePath, float64(fi.Size())/1024/1024/elapsedTime.Seconds(), elapsedTime.Seconds(), float64(fi.Size())/1024/1024, linesCt, longErrCt)
}

func lineDebug() {
	logger.SetLevel(logrus.TraceLevel)
	logger.Debug("Line Debug.....")

	var lines []string
	var appLines []string
	var exceptLines []string
	var testLines []string

	appLines = []string{
		"https://zayiraldeenu@gmail.com:Ah1236789:Application::Google_[Chrome]_Profile:6",
		"https://accounts.google.com/signin/v2/challenge/pwd:mommyhacker1s2sk@gmail.com:Google_[Chrome]_Default",
		"https://wszy%+*w_*7YSD#123:Application::Google_[Chrome]_Default:===============",
		"https://vktarget.ru/list:amid53532@gmail.com::t1158lNP:Mail.Ru_[Atom]_Default",
		"http://bosslike.ru/login::samoha9898@mail.ru:s777amoha:Google_[Chrome]_Backup",
		"http://ets2mp.com::samoha9898@mail.ru:s777amoha:Google_[Chrome]_Backup:Default",
		"https://wwwbagus@gmail.com:1S@mpai8:Application::Google_[Chrome]_Profile:3",
		"ide:gas picim na:max:idegas123:Google_[Chrome]_Profile:24https://play.prodigygame.com:DylanM15594:dylanj101",
		// 避免误报
		"http://academico.admision.espol.edu.ec/:infosalavarriaronny@gmail.com:SD_[xWIgURJ]",
		"Panmpe_[12]:https://www.roblox.com/my/account:pantelesmpekiares@gmail:com",
	}

	exceptLines = []string{
		"ide:gas picim na:max:idegas123:Google_[Chrome]_Profile:24https://play.prodigygame.com:DylanM15594:dylanj101",
		"https://redfin.id.rapidplex.com:2083::agriline:9)Pbhq2W2e*Y1X:",

		// 异常情况，解析错误不用处理
		"https://redfin.id.rapidplex.com:2083|Domains:1|Listagriline.id:agriline:9)Pbhq2W2e*Y1X",
		"https://redfin.id.rapidplex.com:2083 ;https://google.com agriline:9)Pbhq2W2e*Y1X",
		"https://redfin.id.rapidplex.com:2083|Domains 2|Listsch.legalitas.site,legalitas.site legalit6::YV00xfcK5Yc6#",
		"https://ruangguree.com 2083 | Domains: 8 | List akademiaudit.or.id, chressindogroup.com, phria.or.id, fkmonline-usm.com, coaching.ruangguree.com, codesk.ruangguree.com, instruktur.ruangguree.com, ruangguree.com ruae1843:Erwin212077",
	}

	testLines = []string{
		"https://cabinet.instaforex.com/client/ms/login  10168303:dfb36ea3",
		"https://accounts.google.com  ong.makhtoutat:tichit123",
		"https://accounts.google.com/SignUp;ong.makhtoutat:tichit123",
		"http://ajaymachine 8083/saggst/userhome.htm Login::avijit_saha2017:Agartala@121",
		"http://localhost 8083/gengst/gst/client_clientlist Login::avijit_saha2017:Agartala@121",
	}

	lines = appLines
	lines = exceptLines
	lines = testLines

	for _, line := range lines {
		parsedData := parseOneLine(line)
		logger.Debugf("[result]sep[%s] user[%s] pass[%s] url[%s] %s\n",
			parsedData.Separator,
			parsedData.Username,
			parsedData.Password,
			parsedData.URL,
			line)
	}
}

//  end debug

func main() {

	var dirPath string
	var updateDateStr string

	var table string

	// go func() {
	// 	http.ListenAndServe("0.0.0.0:8899", nil)
	// }()

	initEnv()
	initBloom()

	// table = "testparse"
	table = "urluserpass"

	args := os.Args
	dirPath = args[1]
	updateDateStr = args[2]

	// dirPath = "/mnt/e/Link_Login_Pass ( FREE CLOUD LOGS )/2024_03tmp"
	// updateDateStr = "2024-04-01"

	logger.Debug(table, dirPath, updateDateStr)
	processDirMain(table, dirPath, updateDateStr)

	// debug
	// lineDebug()
	// fileDebug()

}
