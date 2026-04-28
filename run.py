import datetime
import json
import os
import random
import subprocess
import time
import zipfile
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from debug_tools import debug_on_exception, http_get

# 这些目录用于缓存接口返回值、保存长文、图片、视频和评论数据。
Path("cache").mkdir(exist_ok=True)
Path("ext/comment").mkdir(parents=True, exist_ok=True)
Path("ext/longtext").mkdir(parents=True, exist_ok=True)
Path("resources/pic").mkdir(parents=True, exist_ok=True)
Path("resources/video").mkdir(parents=True, exist_ok=True)

HEADERS = {
    "authority": "m.weibo.cn",
    "accept": "application/json, text/plain, */*",
    "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
    "cache-control": "no-cache",
    "mweibo-pwa": "1",
    "origin": "https://m.weibo.cn",
    "pragma": "no-cache",
    "referer": "https://m.weibo.cn/compose/",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-ch-ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
    "sec-ch-ua-mobile": "?1",
    "sec-ch-ua-platform": '"Android"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": "Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/116.0.0.0 Mobile Safari/537.36",
    "x-requested-with": "XMLHttpRequest",
}


# 读取 cookie.json 里的登录态，后面所有接口请求都依赖它。
cookie: dict = json.load(open("cookie.json", "r", encoding="utf-8"))
cookie["MLOGIN"] = 1


cache_dir = Path("cache")
cache_dir.mkdir(exist_ok=True)


VIDEO_URL_PRIORITY = [
    "mp4_1080p_mp4",
    "mp4_720p_mp4",
    "mp4_hd_mp4",
    "mp4_ld_mp4",
    "mp4_sd_mp4",
]


def _cookie_header() -> str:
    return "; ".join([f"{k}={v}" for k, v in cookie.items()])


def _media_headers(referer: str = "https://weibo.com/") -> dict:
    return {
        "referer": referer,
        "user-agent": HEADERS["user-agent"],
        "cookie": _cookie_header(),
    }


def _file_ext_from_url(value: str) -> str:
    if not value:
        return ""
    path = urlparse(value).path
    if "." not in path:
        return ""
    return path.rsplit(".", 1)[-1].lower()


def _video_url_from_post(post: dict) -> str:
    page_info = post.get("page_info") or {}
    urls = page_info.get("urls") or {}
    if not isinstance(urls, dict):
        return ""
    for key in VIDEO_URL_PRIORITY:
        if urls.get(key):
            return urls[key]
    for url in urls.values():
        if url:
            return url
    return ""


def _video_url_is_expired(url: str) -> bool:
    expires = parse_qs(urlparse(url).query).get("Expires", [""])[0]
    if not expires:
        return False
    try:
        return int(expires) <= time.time() + 300
    except ValueError:
        return False


def _is_page_video_post(post: dict) -> bool:
    page_info = post.get("page_info") or {}
    return page_info.get("type") == "video"


def _post_video_filename(post: dict, dirname: str) -> Path:
    return Path(dirname) / "video" / f"{post['id']}.mp4"


def _pic_video_filename(pic: dict, post_id: str, dirname: str) -> Path:
    return Path(dirname) / "video" / f"{post_id}_{pic.get('pid')}.mp4"


def _download_video_url(url: str, filename: Path) -> None:
    ext = _file_ext_from_url(url)
    temp_filename = filename.with_name(f"{filename.stem}.part{filename.suffix}")
    if temp_filename.exists():
        temp_filename.unlink()
    if ext == "mp4":
        resp = http_get(url, headers=_media_headers(), timeout=(30, 60))
        resp.raise_for_status()
        content_type = resp.headers.get("content-type", "").lower()
        if content_type.startswith("text/") or "json" in content_type:
            raise ValueError(f"Unexpected video response content-type: {content_type}")
        temp_filename.write_bytes(resp.content)
        temp_filename.replace(filename)
    else:
        command = [
            "ffmpeg",
            "-y",
            "-headers",
            f"Referer: https://weibo.com/\r\nUser-Agent: {HEADERS['user-agent']}\r\nCookie: {_cookie_header()}\r\n",
            "-i",
            url,
            "-c",
            "copy",
            "-bsf:a",
            "aac_adtstoasc",
            str(temp_filename),
        ]
        subprocess.run(command, check=True)
        temp_filename.replace(filename)


# 统一封装一次原始请求，自动补齐微博请求所需的 cookie 和 XSRF token。
def _request(url: str, custom_headers: dict = {}) -> dict:
    headers = {
        **HEADERS,
        **custom_headers,
        "cookie": _cookie_header(),
        "x-xsrf-token": cookie.get("XSRF-TOKEN", ""),
    }
    return http_get(url, headers=headers, timeout=(30, 60)).json()


# 业务级请求封装：支持缓存、自动重试、验证码处理，以及提取 data 字段。
def request(url: str, referer: str = "", cached: bool = False, all_ret=False) -> dict:
    cache_file = cache_dir / f"{url.split('/')[-1].replace('?','_')}.json"
    if cached and cache_file.exists():
        return json.load(cache_file.open("r", encoding="utf-8"))
    headers = {"referer": referer} if referer else {}
    resp = _request(url, headers)
    time.sleep(random.random() * 0.3 + 0.7)
    if "ok" not in resp:
        print(f'[?] {resp}')
        raise NotImplementedError
    if resp["ok"] != 1:
        if resp.get("msg", "") in ["已过滤部分评论", "快来发表你的评论吧", "还没有人评论哦~快来抢沙发！", "因存在疑似骚扰内容，已过滤部分评论"]:
            pass
        else:
            print(f'[?] {resp}')
            refresh_cookie()
            resp = _request(url, headers)
    while resp.get("code") == -100 or resp.get("ok") == -100:
        print(f'[?] {resp}')
        if resp.get("url"):
            print("[!] Weibo requested CAPTCHA verification. Open this URL and finish it:")
            print(resp["url"])
        else:
            print("[!] Weibo requested CAPTCHA verification.")
        input("[!] Press Enter after you finish verification to retry...")
        resp = _request(url, headers)
        time.sleep(random.random() * 0.3 + 0.7)
    if not all_ret:
        resp = resp.get("data", {})
    if cached:
        # 检测疑似限流空页 / 真实最后一页：有 total 但无 since_id 且没有真实帖子（card_type=9）。
        # 两者响应结构相同，无法仅凭响应区分；统一不写缓存，让上层按"最后一页"正常结束循环。
        # 这样：真实结尾下次运行也只多请求一次（无副作用）；限流情况下也不会污染缓存。
        ci = resp.get("cardlistInfo", {})
        if ci.get("total") and "since_id" not in ci:
            real_posts = [c for c in resp.get("cards", []) if c.get("card_type") == 9]
            if not real_posts:
                print(f"[!] 收到终止页（total={ci['total']}，无 since_id 且无帖子）。可能是真实结尾，也可能是限流；本次不写缓存。")
                return resp
        json.dump(resp, cache_file.open("w", encoding="utf-8"), ensure_ascii=False)
    return resp


# 刷新 cookie 中的时间水位和 XSRF token，同时检查当前 cookie 是否仍然有效。
@debug_on_exception
def refresh_cookie(return_uid=False):
    cookie["_T_WM"] = int(time.time() / 3600) * 100001
    resp = _request("https://m.weibo.cn/api/config")
    resp = resp.get("data", {})
    cookie["XSRF-TOKEN"] = resp["st"]

    print(f"[-] Cookie Refreshed")
    print(f"Time watermark: {cookie['_T_WM']}")
    print(f"XSRF token: {cookie['XSRF-TOKEN']}")

    if not resp.get("login", False):
        print("[!] Cookie 可能无效，请检查 cookie.json 文件")
        raise ValueError("Invalid cookie")

    if return_uid:
        return resp["uid"]


# 先用当前 cookie 取到自己的 UID。
UID = refresh_cookie(return_uid=True)
# 如果你想爬取别人的微博，直接修改这里的 UID 即可
# UID = 1111681197

# 通过个人主页接口拿到 container id，后续分页抓取微博会用到它。
more_url = request(
    f"https://m.weibo.cn/profile/info?uid={UID}",
    referer=f"https://m.weibo.cn/profile/{UID}",
)["more"]

CID = int(more_url.split("/")[-1].split("_")[0])


# ====================================================================================================


# 重新拉取一条微博的完整数据，用于处理视频等需要刷新后的字段。
@debug_on_exception
def fetchRefreshedPost(post) -> dict:
    pid = post["id"]
    return request(
        f"https://m.weibo.cn/statuses/show?id={pid}",
        referer=f"https://m.weibo.cn/detail/{pid}",
    )


# 拉取长微博正文，并做本地缓存，避免重复请求。
@debug_on_exception
def fetchLongText(post, dirname) -> None:
    pid = post["id"]
    filename = f"{dirname}/longtext/{pid}.json"
    if Path(filename).exists():
        post["longtext"] = json.load(open(filename, "r", encoding="utf-8"))
        return
    longtext = request(
        f"https://m.weibo.cn/statuses/extend?id={pid}",
        referer=f"https://m.weibo.cn/detail/{pid}",
    ).get("longTextContent", "")
    json.dump(longtext, open(filename, "w", encoding="utf-8"), ensure_ascii=False)
    post["longtext"] = longtext


# 下载微博图片；如果是 livephoto 或视频缩略图，也会按类型分别处理。
@debug_on_exception
def fetchPhoto(pic: dict, post_id: str, dirname) -> None:
    pid = pic.get("pid")
    url = pic.get("large", {}).get("url", "")
    ext = _file_ext_from_url(url)
    if ext in ["jpg", "jpeg", "png", "gif", "webp"]:
        filename = f"{dirname}/pic/{post_id}_{pid}.{ext}"
        if not Path(filename).exists():
            print("[+] Downloading Photo", pid, "from", url)
            resp = http_get(url, headers=_media_headers())
            resp.raise_for_status()
            open(filename, "wb").write(resp.content)
    elif pic.get("type") in ["livephoto", "video", "gifvideos"]:
        print("[!] Skipping invalid photo thumbnail", pid, "from", url)
    else:
        print(f'[?] {pic}')
        raise NotImplementedError(f"Unsupported photo url format: {url}")

    if "type" not in pic:
        return
    if pic["type"] == "livephoto":
        # https://video.weibo.com/media/play?livephoto=https%3A%2F%2Flivephoto.us.sinaimg.cn%2Fxxxxxxxxxxxxxxxx.mov
        url = pic["videoSrc"]
        # ext = url.split("?")[0].split(".")[-1]
        if ".mov" in url:
            filename = f"{dirname}/pic/{post_id}_{pid}.mov"
        else:
            print(f'[?] {pic}')
            raise NotImplementedError(f"Unsupported live photo url format: {url}")
        if not Path(filename).exists():
            print("[+] Downloading Live Photo", pid, "from", url)
            resp = http_get(url, headers=_media_headers())
            resp.raise_for_status()
            open(filename, "wb").write(resp.content)
    elif pic["type"] == "video":
        # https://f.video.weibocdn.com/o0/xxxxxxxxxxxxxxxx.mp4?label=...
        url = pic["videoSrc"]
        filename = _pic_video_filename(pic, post_id, dirname)
        if not filename.exists():
            print("[+] Downloading Video", pid, "from", url)
            _download_video_url(url, filename)
    elif pic["type"] == "gifvideos":
        pass
    else:
        print(f'[?] {pic}')
        raise NotImplementedError(f"Unsupported photo type: {pic['type']}")


# 下载微博正文里单独挂载的视频。
@debug_on_exception
def fetchVideo(post, dirname) -> None:
    pid = post["id"]
    filename = _post_video_filename(post, dirname)
    if filename.exists():
        return
    url = _video_url_from_post(post)
    if not url or _video_url_is_expired(url):
        refreshed_post = fetchRefreshedPost(post)
        post["page_info"] = refreshed_post.get("page_info", post.get("page_info"))
        url = _video_url_from_post(post)
    if not url:
        print("[!] Skipping Video", pid, "because no downloadable url was found")
        return
    print("[+] Downloading Video", pid, "from", url)
    try:
        _download_video_url(url, filename)
    except Exception:
        refreshed_post = fetchRefreshedPost(post)
        refreshed_url = _video_url_from_post(refreshed_post)
        if refreshed_url and refreshed_url != url:
            post["page_info"] = refreshed_post.get("page_info", post.get("page_info"))
            print("[+] Retrying Video", pid, "from refreshed url", refreshed_url)
            _download_video_url(refreshed_url, filename)
        else:
            raise


# 抓取一级评论下的二级评论，并分页缓存到本地。
@debug_on_exception
def fetchSecondComments(mid, cid, max_id, dirname) -> tuple[list, int]:
    if int(max_id) == 0:
        filename = f"{dirname}/comment/{mid}_{cid}.json"
    else:
        filename = f"{dirname}/comment/{mid}_{cid}_{max_id}.json"
    if Path(filename).exists():
        data = json.load(open(filename, "r", encoding="utf-8"))
    else:
        print("[+] Downloading Comment Child", cid, max_id)
        url = f"https://m.weibo.cn/comments/hotFlowChild?cid={cid}&max_id={max_id}&max_id_type=0"
        data = request(url, all_ret=True)
        json.dump(data, open(filename, "w", encoding="utf-8"), ensure_ascii=False)
    if "data" not in data:
        if data["errno"] == "100011" and data["msg"] == "暂无数据":
            return [], 0
        print(f'[?] data')
        raise NotImplementedError
    comments = data["data"]
    max_id = data["max_id"]
    return comments, max_id


# 抓取微博的一级评论，并在遇到“楼中楼”时继续补抓二级评论。
@debug_on_exception
def fetchFirstComments(mid, max_id, dirname) -> tuple[list, int]:
    if int(max_id) == 0:
        filename = f"{dirname}/comment/{mid}.json"
    else:
        filename = f"{dirname}/comment/{mid}_{max_id}.json"
    if Path(filename).exists():
        data = json.load(open(filename, "r", encoding="utf-8"))
    else:
        print("[+] Downloading Comment", mid, max_id)
        url = f"https://m.weibo.cn/comments/hotflow?mid={mid}&max_id={max_id}&max_id_type=0"
        data = request(url, all_ret=True)
        json.dump(data, open(filename, "w", encoding="utf-8"), ensure_ascii=False)
    if "data" not in data:
        return [], 0
    data = data["data"]
    comments = []
    for x in data["data"]:
        if x["comments"] and x["total_number"] != len(x["comments"]):
            comments_all = []
            _max_id = 0
            while True:
                _data, _max_id = fetchSecondComments(mid, x["id"], _max_id, dirname)
                comments_all += _data
                if _max_id == 0:
                    break
            x["comments_all"] = comments_all
        comments.append(x)
    max_id = data["max_id"]
    return comments, max_id


# 把一条微博的全部评论整理到 post["comments"] 里。
@debug_on_exception
def fetchComments(post, dirname) -> None:
    mid = post["mid"]
    if post["comments_count"] == 0:
        post["comments"] = []
        return
    max_id = 0
    comments = []
    while True:
        _comments, max_id = fetchFirstComments(mid, max_id, dirname)
        comments += _comments
        if max_id == 0:
            break
    post["comments"] = comments


# 一条微博的完整补全流程：长文、视频、图片、评论都在这里统一抓取。
@debug_on_exception
def fetchRelatedContent(post):
    # 原创的微博
    if post["isLongText"]:
        fetchLongText(post, "ext")
    if _is_page_video_post(post):
        fetchVideo(post, "resources")
    if "pics" in post:
        for pic in post["pics"]:
            fetchPhoto(pic, post["id"], "resources")
    fetchComments(post, "ext")

    # 转发的微博
    # if "retweeted_status" in post and post["retweeted_status"].get("isLongText", False):
    #     post["retweeted_status"]["longtext"] = fetchLongText(post["retweeted_status"])
    # if "retweeted_status" in post and post["retweeted_status"].get("pics", []):
    #     for pic in post["retweeted_status"]["pics"]:
    #         fetchPhoto(pic)


def backfillMissingVideos(posts, dirname="resources") -> None:
    page_video_posts = [
        post
        for post in posts
        if _is_page_video_post(post) and not _post_video_filename(post, dirname).exists()
    ]
    pic_videos = []
    for post in posts:
        for pic in post.get("pics") or []:
            if pic.get("type") == "video" and not _pic_video_filename(pic, post["id"], dirname).exists():
                pic_videos.append((post, pic))
    total = len(page_video_posts) + len(pic_videos)
    if total == 0:
        return
    print(f"[+] Backfilling {total} missing Video files")
    for post in page_video_posts:
        fetchVideo(post, dirname)
    for post, pic in pic_videos:
        fetchPhoto(pic, post["id"], dirname)


# ====================================================================================================


# 增量模式：如果已有 posts.json，只抓新增微博并追加到旧数据里。
@debug_on_exception
def fetchIncrementalPosts(posts=None):
    data = request(
        f"https://m.weibo.cn/api/container/getIndex?containerid={CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
        referer=f"https://m.weibo.cn/p/{CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
    )
    if posts is None:
        posts = json.load(open("posts.json", "r", encoding="utf-8"))
    post_ids = set([post["id"] for post in posts])
    full_scan = os.environ.get("WEIBO_ARCHIVE_FULL_INCREMENTAL_SCAN") == "1"
    while "since_id" in data["cardlistInfo"]:
        since_id: int = data["cardlistInfo"]["since_id"]
        known_posts_on_page = 0
        new_posts_on_page = 0
        for card in data["cards"]:
            if card["card_type"] == 9:
                if card["mblog"]["id"] in post_ids:
                    known_posts_on_page += 1
                    continue
                fetchRelatedContent(card["mblog"])
                posts.append(card["mblog"])
                post_ids.add(card["mblog"]["id"])
                new_posts_on_page += 1
            elif card["card_type"] == 11 and "card_group" in card:
                for sub_card in card["card_group"]:
                    if sub_card["card_type"] == 9:
                        if sub_card["mblog"]["id"] in post_ids:
                            known_posts_on_page += 1
                            continue
                        fetchRelatedContent(sub_card["mblog"])
                        posts.append(sub_card["mblog"])
                        post_ids.add(sub_card["mblog"]["id"])
                        new_posts_on_page += 1
                    else:
                        print("[+] Unknown card type", sub_card["card_type"])
            else:
                print("[+] Unknown card type", card["card_type"])
        print("[+]", len(posts), "posts", posts[-1]["created_at"], since_id, f"(+{new_posts_on_page})")
        if known_posts_on_page and new_posts_on_page == 0 and not full_scan:
            print("[+] 已遇到已归档微博，增量检查结束")
            return posts
        # since_id 是服务端翻页游标，不是可靠的微博 ID；默认只扫到已归档页。
        # 如需修补历史缺口，可设置 WEIBO_ARCHIVE_FULL_INCREMENTAL_SCAN=1 继续向旧页扫描。
        time.sleep(random.random() * 1.0 + 2.0)  # 翻页间额外延迟 2~3 秒，降低限流风险
        data = request(
            f"https://m.weibo.cn/api/container/getIndex?containerid={CID}_-_WEIBO_SECOND_PROFILE_WEIBO&page_type=03&since_id={since_id}",
            referer=f"https://m.weibo.cn/p/{CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
            cached=True,
        )

    # last page case
    if data["cardlistInfo"].get("since_id"):
        return posts
    for card in data["cards"]:
        if card["card_type"] == 9:
            if card["mblog"]["id"] in post_ids:
                continue
            fetchRelatedContent(card["mblog"])
            posts.append(card["mblog"])
        elif card["card_type"] == 11 and "card_group" in card:
            for sub_card in card["card_group"]:
                if sub_card["card_type"] == 9:
                    if sub_card["mblog"]["id"] in post_ids:
                        continue
                    fetchRelatedContent(sub_card["mblog"])
                    posts.append(sub_card["mblog"])
                else:
                    print("[+] Unknown card type", sub_card["card_type"])
        else:
            print("[+] Unknown card type", card["card_type"])
    if not posts:
        print("[+]", "No posts found!")
        return posts
    print("[+]", len(posts), "posts", posts[-1]["created_at"], "last page!")
    return posts


# 全量模式：从头分页抓取所有微博，并顺手补全相关资源。
@debug_on_exception
def fetchPosts():
    if Path("posts.json").exists():
        posts = json.load(open("posts.json", "r", encoding="utf-8"))
        backfillMissingVideos(posts)
        print("[-] 检测到 posts.json 文件，将进行增量备份")
        return fetchIncrementalPosts(posts)
    data = request(
        f"https://m.weibo.cn/api/container/getIndex?containerid={CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
        referer=f"https://m.weibo.cn/p/{CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
        cached=True,
    )
    posts = []
    while "since_id" in data["cardlistInfo"]:
        since_id = data["cardlistInfo"]["since_id"]
        for card in data["cards"]:
            if card["card_type"] == 9:
                fetchRelatedContent(card["mblog"])
                posts.append(card["mblog"])
            elif card["card_type"] == 11 and "card_group" in card:
                for sub_card in card["card_group"]:
                    if sub_card["card_type"] == 9:
                        fetchRelatedContent(sub_card["mblog"])
                        posts.append(sub_card["mblog"])
                    else:
                        print("[+] Unknown card type", sub_card["card_type"])
            else:
                print("[+] Unknown card type", card["card_type"])
        print("[+]", len(posts), "posts", posts[-1]["created_at"], since_id)
        time.sleep(random.random() * 1.0 + 2.0)  # 翻页间额外延迟 2~3 秒，降低限流风险
        data = request(
            f"https://m.weibo.cn/api/container/getIndex?containerid={CID}_-_WEIBO_SECOND_PROFILE_WEIBO&page_type=03&since_id={since_id}",
            referer=f"https://m.weibo.cn/p/{CID}_-_WEIBO_SECOND_PROFILE_WEIBO",
            cached=True,
        )

    # last page case:
    for card in data["cards"]:
        if card["card_type"] == 9:
            fetchRelatedContent(card["mblog"])
            posts.append(card["mblog"])
        elif card["card_type"] == 11 and "card_group" in card:
            for sub_card in card["card_group"]:
                if sub_card["card_type"] == 9:
                    fetchRelatedContent(sub_card["mblog"])
                    posts.append(sub_card["mblog"])
                else:
                    print("[+] Unknown card type", sub_card["card_type"])
        else:
            print("[+] Unknown card type", card["card_type"])
    if not posts:
        print("[+]", "No posts found!")
        return posts
    print("[+]", len(posts), "posts", posts[-1]["created_at"], "last page!")
    return posts


if __name__ == "__main__":
    # 主入口：抓取微博 -> 排序 -> 保存 posts.json -> 打包成 zip 归档。
    posts = fetchPosts()
    print("Total", len(posts), "posts")

    posts = sorted(posts, key=lambda x: x["id"], reverse=True)
    print(f"[+] Saving into posts.json")
    json.dump(posts, open("posts.json", "w", encoding="utf-8"), ensure_ascii=False)

    # 把目录下的文件递归加入压缩包，保留相对路径。
    def zipdir(path, ziph):
        for root, dirs, files in os.walk(path):
            for file in files:
                ziph.write(
                    os.path.join(root, file),
                    os.path.relpath(os.path.join(root, file), os.path.join(path, "..")),
                )

    current_date = datetime.datetime.now().strftime("%Y%m%d")
    zip_filename = f"weibo_archive_{current_date}.zip"
    print(f"[+] Saving into {zip_filename}")
    zipf = zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED)
    zipf.write("posts.json")
    folders = ["ext", "resources"]
    for folder in folders:
        zipdir(folder, zipf)
    zipf.close()
