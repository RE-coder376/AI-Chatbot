import requests, time, sys
sys.stdout.reconfigure(encoding='utf-8')

API   = 'http://localhost:8000'
DELAY = 15
RL_WAIT = 75  # wait this many seconds when rate-limited (longer than key cooldown)

def ask(q):
    r = requests.post(f'{API}/chat', json={'question':q,'history':[],'stream':False}, timeout=40)
    return r.json().get('answer','ERROR')

def switch(db):
    r = requests.post(f'{API}/admin/databases/set-active',
        data={'password':'admin123','name':db}, timeout=15)
    if r.status_code != 200:
        print(f'  [SWITCH FAILED] HTTP {r.status_code}: {r.text[:80]}')
        return False
    # Wait for init_systems() to reload the DB and status to be ready
    for _ in range(15):
        time.sleep(2)
        h = requests.get(f'{API}/health', timeout=5).json()
        if h.get('db') == db and 'ready' in h.get('status', ''):
            return True
    print(f'  [SWITCH TIMEOUT] health still shows db={h.get("db")} status={h.get("status")}')
    return False

def grade(ans, exp):
    a = ans.lower()
    if any(x in a for x in ['unable to respond', 'try again in a moment', 'rate-limited', 'rate limited', 'overloaded']):
        return 'RATE_LIMIT'
    leaked  = any(x in a for x in ['knowledge base','business context','based on the kb','according to the knowledge'])
    idk     = any(x in a for x in ["don't have specific","i don't have",'reach out to the','contact the'])
    refused = any(x in a for x in ['can only assist','only help with','general-purpose assistant','can only help with'])
    issues  = []
    if leaked:                          issues.append('SOURCE_LEAK')
    if exp=='ANSWER' and idk:           issues.append('WRONG_IDK')
    if exp=='ANSWER' and refused:       issues.append('WRONG_REFUSE')
    if exp=='IDK'    and not idk:       issues.append('SHOULD_IDK')
    if exp=='REFUSE' and not refused:   issues.append('SHOULD_REFUSE')
    return 'PASS' if not issues else 'FAIL[' + ','.join(issues) + ']'

tests = {
  'agentfactory': [
    ('What is AgentFactory?',                                  'ANSWER'),
    ('Who is this curriculum for?',                            'ANSWER'),
    ('What is the agents-as-tools pattern?',                   'ANSWER'),
    ('What does Andrew Ng say about building agent workflows?','ANSWER'),
    ('How does an orchestrator manage specialist agents?',     'ANSWER'),
    ('How much does the course cost?',                         'IDK'),
    ('What programming language is used in the lessons?',      'IDK'),
    ('Is there a certificate after completing the course?',    'IDK'),
    ('What is the weather in Karachi today?',                  'REFUSE'),
    ('Write me a Python script to scrape Twitter.',            'REFUSE'),
  ],
  'books': [
    ('What kind of books do you have?',                        'ANSWER'),
    ('What is your library called?',                           'ANSWER'),
    ('Can you help me find a good book?',                      'ANSWER'),
    ('Do you have any fiction novels?',                        'ANSWER'),
    ('What reading material is available?',                    'ANSWER'),
    ('What are your opening hours?',                           'IDK'),
    ('How much is a library membership?',                      'IDK'),
    ('Can I return a book after two weeks?',                   'IDK'),
    ('How do I invest in the stock market?',                   'REFUSE'),
    ('What is the capital of Japan?',                          'REFUSE'),
  ],
  'default': [
    ('What does Sapphire sell?',                               'ANSWER'),
    ('Do you sell womens clothing?',                           'ANSWER'),
    ('How can I track my order?',                              'ANSWER'),
    ('Can I send back something I bought?',                    'ANSWER'),
    ('Do you deliver for free?',                               'ANSWER'),
    ('What is the price of a specific dress?',                 'IDK'),
    ('Where are your physical stores located?',                'IDK'),
    ('When was Sapphire founded?',                             'IDK'),
    ('How do I bake a chocolate cake?',                        'REFUSE'),
    ('Who won the FIFA World Cup?',                            'REFUSE'),
  ],
}

total_p = total_f = total_r = 0

for db, qs in tests.items():
    print(f'\n--- {db.upper()} ---')
    switch(db)
    p = f = r = 0
    for q, exp in qs:
        time.sleep(DELAY)
        ans = ask(q)
        v   = grade(ans, exp)
        short = ans[:110].replace('\n', ' ')
        if v == 'RATE_LIMIT':
            r += 1
            print(f'  [RATE_LIMIT — waiting {RL_WAIT}s] {exp} | {q[:50]}')
            time.sleep(RL_WAIT)
            # Retry once after cooldown
            ans = ask(q)
            v   = grade(ans, exp)
            short = ans[:110].replace('\n', ' ')
            if v == 'RATE_LIMIT':
                print(f'  [RATE_LIMIT RETRY FAILED — skipping] {exp} | {q[:50]}')
                continue
        if v == 'PASS':
            p += 1
        else:
            f += 1
        print(f'  [{v:38s}] {exp:6s} | {q[:50]}')
        if v != 'PASS':
            print(f'    A: {short}')
    print(f'  => {p} pass  {f} fail  {r} rate-limited')
    total_p += p
    total_f += f
    total_r += r

print(f'\n=== COMPLETE: {total_p} PASS / {total_f} FAIL / {total_r} RATE_LIMIT / 30 ===')
