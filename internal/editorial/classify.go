package editorial

import (
	"strings"

	"github.com/jxrory/newspulse/internal/config"
	"github.com/jxrory/newspulse/internal/storage"
)

const (
	SectionInternational = storage.SectionInternational
	SectionDomestic      = storage.SectionDomestic
	SectionStatus        = storage.SectionStatus
)

var internationalKeywords = []string{
	"国际", "全球", "海外", "外交", "联合国", "北约", "欧盟",
	"美国", "华盛顿", "白宫", "特朗普", "拜登", "欧洲", "英国", "法国",
	"德国", "俄罗斯", "乌克兰", "中东", "以色列", "伊朗", "巴勒斯坦",
	"日本", "韩国", "朝鲜", "印度", "东南亚", "非洲", "拉美", "巴西",
	"加拿大", "澳大利亚", "新西兰", "墨西哥", "阿根廷", "土耳其",
	"沙特", "阿联酋", "卡塔尔", "埃及", "南非", "意大利", "西班牙",
	"荷兰", "瑞典", "挪威", "芬兰", "波兰", "捷克", "匈牙利",
}

var domesticKeywords = []string{
	"国务院", "发改委", "央行", "人民银行", "教育部", "公安部",
	"财政部", "商务部", "工信部", "农业农村部", "生态环境部",
	"住建部", "交通运输部", "水利部", "文化和旅游部", "国家统计局",
	"全国人大", "全国政协", "中共中央", "省委", "省政府", "市委",
	"市政府", "县委", "县政府", "国内", "我国", "人民币", "A股",
	"沪深", "中国内地", "内地",
}

// Classify assigns the digest section from a source hint plus a conservative
// title/summary heuristic. Mixed finance feeds may carry international news;
// the rules intentionally keep "China" alone from forcing domestic.
func Classify(src config.SourceConfig, title, summary string) string {
	switch src.SectionHint {
	case "international":
		return "international"
	case "domestic":
		return "domestic"
	}
	text := title + " " + summary
	if src.Language != "zh" {
		return "international"
	}
	intl, dom := 0, 0
	for _, k := range internationalKeywords {
		if strings.Contains(text, k) {
			intl++
		}
	}
	for _, k := range domesticKeywords {
		if strings.Contains(text, k) {
			dom++
		}
	}
	if strings.Contains(text, "中国") || strings.Contains(text, "中方") {
		// Chinese diplomacy with a foreign counterpart is still one event;
		// prefer international only when foreign signals dominate.
		dom++
	}
	if intl > dom {
		return "international"
	}
	return "domestic"
}

// SectionLabel renders the Chinese section heading used in digests.
func SectionLabel(section string) string {
	switch section {
	case "international":
		return "国际新闻"
	case "domestic":
		return "中国国内新闻"
	case "status":
		return "状态"
	default:
		return section
	}
}
