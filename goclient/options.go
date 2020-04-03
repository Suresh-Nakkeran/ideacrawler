package goclient

import (
	pb "github.com/Suresh-Nakkeran/ideacrawler/protofiles"
)

type Option func(*JobSpec)

func SeedURL(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.SeedUrl = vv
	}
}

func MinDelay(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.MinDelay = vv
	}
}

func MaxDelay(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.MaxDelay = vv
	}
}

func MaxIdleTime(vv int64) Option {
	return func(args *JobSpec) {
		args.dopt.MaxIdleTime = vv
	}
}

func ThreadsPerSite(vv int64) Option {
	return func(args *JobSpec) {
		args.dopt.ThreadsPerSite = vv
	}
}

func NoFollow() Option {
	return func(args *JobSpec) {
		args.dopt.NoFollow = true
	}
}

func CallbackURLRegexp(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.CallbackUrlRegexp = vv
	}
}

func FollowURLRegexp(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.FollowUrlRegexp = vv
	}
}

func MaxConcurrentRequests(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.MaxConcurrentRequests = vv
	}
}

func Useragent(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.Useragent = vv
	}
}

func Impolite() Option {
	return func(args *JobSpec) {
		args.dopt.Impolite = true
	}
}

func Depth(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.Depth = vv
	}
}

func UnsafeNormalizeURL() Option {
	return func(args *JobSpec) {
		args.dopt.UnsafeNormalizeURL = true
	}
}

func CheckLoginAfterEachPage() Option {
	return func(args *JobSpec) {
		args.dopt.CheckLoginAfterEachPage = true
	}
}

func Chrome(uu bool, vv string) Option {
	return func(args *JobSpec) {
		args.dopt.Chrome = uu
		args.dopt.ChromeBinary = vv
	}
}

func DomLoadTime(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.DomLoadTime = vv
	}
}

func NetworkIface(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.NetworkIface = vv
	}
}

func CancelOnDisconnect() Option {
	return func(args *JobSpec) {
		args.dopt.CancelOnDisconnect = true
	}
}

func CheckContent() Option {
	return func(args *JobSpec) {
		args.dopt.CheckContent = true
	}
}

func Prefetch() Option {
	return func(args *JobSpec) {
		args.dopt.Prefetch = true
	}
}

func CallbackAnchorTextRegexp(vv string) Option {
	return func(args *JobSpec) {
		args.dopt.CallbackAnchorTextRegexp = vv
	}
}

func CleanUpFunc(vv func()) Option {
	return func(args *JobSpec) {
		args.cleanUpFunc = vv
	}
}

func PageChan(vv chan *pb.PageHTML) Option {
	return func(args *JobSpec) {
		args.implPageChan = vv
	}
}

func AnalyzedURL() Option {
	return func(args *JobSpec) {
		args.useAnalyzedURLChan = true
	}
}

func AnalyzedURLChan(vv chan *pb.UrlList) Option {
	return func(args *JobSpec) {
		args.useAnalyzedURLChan = true
		args.implAnalyzedURLChan = vv
	}
}

func CallbackSeedURL() Option {
	return func(args *JobSpec) {
		args.dopt.CallbackSeedUrl = true
	}
}

func Login(loginUrl string, loginPayload, loginParseXpath KVMap, loginSuccessCheck KVMap) Option {
	return func(args *JobSpec) {
		args.dopt.Login = true
		args.dopt.LoginUrl = loginUrl
		for k, v := range loginPayload {
			args.dopt.LoginPayload = append(args.dopt.LoginPayload, &pb.KVP{Key: k, Value: v})
		}
		if len(loginParseXpath) > 0 {
			args.dopt.LoginParseFields = true
		}
		for k, v := range loginParseXpath {
			args.dopt.LoginParseXpath = append(args.dopt.LoginParseXpath, &pb.KVP{Key: k, Value: v})
		}

		for k, v := range loginSuccessCheck {
			args.dopt.LoginSuccessCheck = &pb.KVP{Key: k, Value: v}
		}
	}
}

func LoginChrome(loginUrl string, loginJS string, loginSuccessCheck KVMap) Option {
	return func(args *JobSpec) {
		args.dopt.Login = true
		args.dopt.LoginUrl = loginUrl
		args.dopt.LoginJS = loginJS
		for k, v := range loginSuccessCheck {
			args.dopt.LoginSuccessCheck = &pb.KVP{Key: k, Value: v}
		}
	}
}

func CallbackXpathMatch(mdata KVMap) Option {
	return func(args *JobSpec) {
		for k, v := range mdata {
			args.dopt.CallbackXpathMatch = append(args.dopt.CallbackXpathMatch, &pb.KVP{Key: k, Value: v})
		}
	}
}

func CallbackXpathRegexp(mdata KVMap) Option {
	return func(args *JobSpec) {
		for k, v := range mdata {
			args.dopt.CallbackXpathRegexp = append(args.dopt.CallbackXpathRegexp, &pb.KVP{Key: k, Value: v})
		}
	}
}

func DisableImages() Option {
	return func(args *JobSpec) {
		args.dopt.DisableImages = true
	}
}

func ScrollCount(vv int32) Option {
	return func(args *JobSpec) {
		args.dopt.ScrollCount = vv
	}
}
