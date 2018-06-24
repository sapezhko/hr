import collections
import csv
import json
import signal
import logging
import asyncio
import aiohttp
import async_timeout


DEFAULT_CONFIG_PATH = './config.json'
DEFAULT_HTTP_TIMEOUT = 60
DEFAULT_PER_PAGE_LIMIT = 100
DEFAULT_RATE_LIMIT_TIMEOUT = 60
GITHUB_API_TEMPLATE = 'https://api.github.com/{}'
GITHUB_API_REPO_SUMMARY_TEMPLATE = GITHUB_API_TEMPLATE.format('repos/{}?access_token={}')
GITHUB_API_REPO_COMMITS_TEMPLATE = GITHUB_API_TEMPLATE.format('repos/{}/commits?access_token={}&per_page={}&page={}')

config = {}
commiters_cache = {}


async def load_with_retry(session, url):
    while True:
        success = True
        with async_timeout.timeout(DEFAULT_HTTP_TIMEOUT):
            async with session.get(url) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    success = False
                    logging.error('Got %d status', resp.status)
        if success:
            break
        else:
            await asyncio.sleep(DEFAULT_RATE_LIMIT_TIMEOUT)


async def filter_commiters(session, commits):
    retval = []
    for commit in commits:
        if commit['author'] is None or 'login' not in commit['author']:
            continue
        login = commit['author']['login']
        url = '{}?access_token={}'.format(commit['author']['url'], config['accessToken'])
        if login not in commiters_cache:
            profile_data = await load_with_retry(session, url)
            commiters_cache[login] = profile_data
        else:
            profile_data = commiters_cache[login]
        for loc in config['locations']:
            if profile_data['location'] is not None and loc.lower() in profile_data['location'].lower():
                retval.append(login)
                break
    return retval


def basic_setup():
    setup_logger(config['logLevel'])


def setup_logger(log_level):
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.getLevelName(log_level))


def load_config():
    global config
    with open(DEFAULT_CONFIG_PATH, 'r') as fp:
        config = json.load(fp)


def sigterm_handler(loop):
    loop.remove_signal_handler(signal.SIGTERM)
    loop.stop()


async def worker(repo):
    logging.info('Start processing %s', repo)
    commiters = collections.Counter()
    summary_url = GITHUB_API_REPO_SUMMARY_TEMPLATE.format(repo, config['accessToken'])
    logging.info('Load summary info for %s. URL: %s', repo, summary_url)
    async with aiohttp.ClientSession() as session:
        repo_summary = await load_with_retry(session, summary_url)
        logging.debug('Loaded summary for %s. Name: "%s", Description "%s"', repo, repo_summary['name'], repo_summary['description'])
        page = 0
        while True:
            page = page + 1
            url = GITHUB_API_REPO_COMMITS_TEMPLATE.format(repo, config['accessToken'], DEFAULT_PER_PAGE_LIMIT, page)
            logging.debug(url)
            data = await load_with_retry(session, url)
            if len(data) == 0:
                break
            new_commiters = await filter_commiters(session, data)
            for commiter in new_commiters:
                commiters[commiter] += 1
    logging.info("Finished processing %s", repo)
    return {
        'repo': repo,
        'name': repo_summary['name'],
        'description': repo_summary['description'],
        'commiters': commiters
    }


def out_to_csv(data):
    filename = '{}.csv'.format(data['name'])
    logging.info('Writing %s', filename)
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['login', 'name', 'commit_count', 'url']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for commiter, commit_count in data['commiters'].items():
            profile = commiters_cache[commiter]
            writer.writerow({'login': profile['login'],
                             'name': profile['name'],
                             'commit_count': commit_count,
                             'url': profile['html_url']})


async def main():
    load_config()
    basic_setup()
    workers = []
    for repo in config['repos']:
        workers.append(worker(repo))
    logging.info('Start workers')
    finished, _ = await asyncio.wait(workers)
    logging.info('All workers finished')
    logging.info('Processing results')
    for fut in finished:
        out_to_csv(fut.result())


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, sigterm_handler, loop)

    try:
        loop.run_until_complete(main())
    except KeyboardInterrupt:
        pass
    finally:
        loop.close()
