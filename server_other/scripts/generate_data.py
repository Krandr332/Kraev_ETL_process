import random
from datetime import datetime, timedelta
from pymongo import MongoClient
from faker import Faker

fake = Faker()
client = MongoClient('mongodb://mongodb:27017/')
db = client['source_db']

db.user_sessions.drop()
db.support_tickets.drop()

users = [fake.uuid4() for _ in range(50)]
sessions = []
for _ in range(200):
    user_id = random.choice(users)
    start = fake.date_time_between(start_date='-30d', end_date='now')
    end = start + timedelta(minutes=random.randint(1, 120))
    pages = random.sample(['/home', '/products', '/products/42', '/cart', '/checkout', '/profile'],
                          k=random.randint(1, 5))
    actions = random.sample(['login', 'view_product', 'add_to_cart', 'remove_from_cart', 'checkout', 'logout'],
                            k=random.randint(0, 4))
    sessions.append({
        'session_id': fake.uuid4(),
        'user_id': user_id,
        'start_time': start.isoformat() + 'Z',
        'end_time': end.isoformat() + 'Z',
        'pages_visited': pages,
        'device': random.choice(['mobile', 'desktop', 'tablet']),
        'actions': actions
    })
db.user_sessions.insert_many(sessions)

tickets = []
for _ in range(100):
    user_id = random.choice(users)
    created = fake.date_time_between(start_date='-60d', end_date='now')
    updated = created + timedelta(hours=random.randint(1, 72))
    status = random.choice(['open', 'in_progress', 'resolved', 'closed'])
    issue_type = random.choice(['payment', 'technical', 'account', 'product', 'other'])
    messages = []
    msg_count = random.randint(1, 5)
    for i in range(msg_count):
        sender = 'user' if i % 2 == 0 else 'support'
        messages.append({
            'sender': sender,
            'message': fake.sentence(),
            'timestamp': (created + timedelta(hours=i*2)).isoformat() + 'Z'
        })
    tickets.append({
        'ticket_id': fake.uuid4(),
        'user_id': user_id,
        'status': status,
        'issue_type': issue_type,
        'messages': messages,
        'created_at': created.isoformat() + 'Z',
        'updated_at': updated.isoformat() + 'Z'
    })
db.support_tickets.insert_many(tickets)

print("Данные успешно сгенерированы в MongoDB.")