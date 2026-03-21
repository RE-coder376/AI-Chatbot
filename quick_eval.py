import requests, time, sys
sys.stdout.reconfigure(encoding='utf-8')

API   = 'http://localhost:8000'
DELAY = 10

def ask(q):
    r = requests.post(f'{API}/chat', json={'question':q,'history':[],'stream':False}, timeout=40)
    return r.json().get('answer','ERROR')

def switch(db):
    r = requests.post(f'{API}/admin/databases/set-active',
        data={'password':'admin123','name':db}, timeout=15)
    if r.status_code != 200:
        print(f'  [SWITCH FAILED] {r.status_code}'); return
    for _ in range(15):
        time.sleep(2)
        h = requests.get(f'{API}/health', timeout=5).json()
        if h.get('db') == db and 'ready' in h.get('status', ''):
            print(f'  [DB READY] {db} | {h.get("docs_indexed")} docs'); return
    print(f'  [SWITCH TIMEOUT]')

tests = {
  'agentfactory': [
    'What is the difference between orchestrator-worker and pipeline agent architectures, and when should you use each?',
    'How does the agents-as-tools pattern work and how does it differ from standard function calling?',
    'What does Andrew Ng say about the iterative loop in agentic workflows?',
    'What types of memory does an AI agent use and how do they interact during a task?',
    'What is multi-agent collaboration and what problem does it solve in complex AI tasks?',
  ],
  'books': [
    'What genres or categories of books does this library specialise in?',
    'Can you recommend a book for someone interested in philosophy of mind?',
    'What is the most intellectually challenging book in the collection?',
    'Do you have books that explore the relationship between science and society?',
    'Which books in the collection deal with narrative structure or storytelling theory?',
  ],
  'default': [
    'What is Sapphires return and exchange policy, and are there different rules for sale items?',
    'If my order shows delivered but I never received it, what exact steps should I take?',
    'Does Sapphire offer cash on delivery, and are there any conditions or restrictions?',
    'What is the difference between standard and express delivery at Sapphire?',
    'Can I return an item bought online to a physical Sapphire store?',
  ],
}

for db, qs in tests.items():
    print(f'\n{"="*60}')
    print(f'  DB: {db.upper()}')
    print(f'{"="*60}')
    switch(db)
    for i, q in enumerate(qs, 1):
        if i > 1: time.sleep(DELAY)
        ans = ask(q)
        rl = any(x in ans.lower() for x in ['unable to respond','rate-limited','rate limited','overloaded','try again'])
        print(f'\nQ{i}: {q}')
        if rl:
            print(f'  [RATE LIMITED — waiting 75s]')
            time.sleep(75)
            ans = ask(q)
        print(f'A: {ans[:300]}')
        print('-'*40)

print('\n=== DONE ===')
