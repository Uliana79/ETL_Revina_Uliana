db = db.getSiblingDB('admin');
db.auth('admin', 'admin');

db = db.getSiblingDB('service');

function randomDate(start, end) {
    return new Date(start.getTime() + Math.random() * (end.getTime() - start.getTime()));
}

function randomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}

function randomElement(array) {
    return array[Math.floor(Math.random() * array.length)];
}

const START_DATE = new Date('2025-11-01T00:00:00Z');
const END_DATE = new Date('2026-03-05T23:59:59Z');

const PAGES = ['/home', '/products', '/products/42', '/products/123', '/cart', '/checkout', '/about', '/contact'];
const ACTIONS = ['login', 'logout', 'view_product', 'add_to_cart', 'remove_from_cart', 'checkout'];
const EVENT_TYPES = ['click', 'page_view', 'scroll', 'form_submit', 'error'];
const ISSUE_TYPES = ['payment', 'shipping', 'account_access', 'refund', 'technical_issue'];
const TICKET_STATUSES = ['open', 'in_progress', 'resolved', 'closed'];
const PRODUCTS = ['prod_101', 'prod_102', 'prod_103', 'prod_201', 'prod_202', 'prod_301', 'prod_302', 'prod_401'];
const MODERATION_STATUSES = ['pending', 'approved', 'rejected'];
const FLAGS = ['contains_images', 'contains_links', 'spam'];
const DEVICE_TYPES = ['mobile', 'desktop', 'tablet'];

// -------------------------------------------------------------------------------

db.Users.drop();

const users = [];
const registrationDates = [];
for (let i = 1; i <= 300; i++) {
    const regDate = randomDate(new Date('2024-01-01'), END_DATE);
    registrationDates.push(regDate);
    
    users.push({
        user_id: `user_${i}`,
        email: `user${i}@example.com`,
        name: `User ${i}`,
        registration_date: regDate.toISOString(),
        last_active: randomDate(regDate, END_DATE).toISOString(),
        is_active: Math.random() > 0.1 
    });
}
db.Users.insertMany(users);

// -------------------------------------------------------------------------------

db.UserSessions.drop();

const sessions = [];
for (let i = 1; i <= 1500; i++) {
    const startTime = randomDate(START_DATE, END_DATE);
    const endTime = new Date(startTime.getTime() + randomInt(5, 120) * 60000);
    
    // Разное количество посещенных страниц (от 1 до 8)
    const numPages = randomInt(1, 8);
    const pages = [];
    for (let p = 0; p < numPages; p++) {
        pages.push(randomElement(PAGES));
    }
    
    // Разное количество действий (от 1 до 6)
    const numActions = randomInt(1, 6);
    const actions = [];
    for (let a = 0; a < numActions; a++) {
        actions.push(randomElement(ACTIONS));
    }
    
    sessions.push({
        session_id: `sess_${String(i).padStart(3, '0')}`,
        user_id: `user_${randomInt(1, 300)}`,
        start_time: startTime.toISOString(),
        end_time: endTime.toISOString(),
        pages_visited: pages,
        device: randomElement(DEVICE_TYPES),
        actions: actions
    });
}
db.UserSessions.insertMany(sessions);

// -------------------------------------------------------------------------------

db.EventLogs.drop();

const events = [];
for (let i = 1; i <= 1500; i++) {
    events.push({
        event_id: `evt_${String(i).padStart(4, '0')}`,
        timestamp: randomDate(START_DATE, END_DATE).toISOString(),
        event_type: randomElement(EVENT_TYPES),
        details: { [randomElement(PAGES)]: {} }
    });
}
db.EventLogs.insertMany(events);

// -------------------------------------------------------------------------------

db.SupportTickets.drop();

const userMessages = [
    "Не могу оплатить заказ, пишет ошибку",
    "Товар пришел поврежденный",
    "Когда будет доставка? Уже прошла неделя",
    "Хочу вернуть товар, как это сделать?",
    "Не приходит подтверждение на email",
    "Списалась двойная оплата",
    "Товар не соответствует описанию",
    "Не могу войти в аккаунт"
];

const supportMessages = [
    "Здравствуйте! Мы уже проверяем вашу проблему.",
    "Спасибо за обращение, мы скоро свяжемся с вами.",
    "Проверьте, пожалуйста, правильность введенных данных.",
    "Мы передали информацию в технический отдел.",
    "Ваш запрос в обработке, ожидайте ответа в течение часа.",
    "Проблема решена. Проверьте, пожалуйста, функционал.",
    "Мы отправили вам инструкцию на email."
];

const allTickets = [];

for (let ticketNum = 1; ticketNum <= 200; ticketNum++) {
    const ticketId = `ticket_${String(ticketNum).padStart(3, '0')}`;
    const userId = `user_${randomInt(1, 300)}`;
    const issueType = randomElement(ISSUE_TYPES);
    const created = randomDate(START_DATE, END_DATE);
    
    // Определяем "судьбу" тикета - сколько статусов он пройдет
    const fate = Math.random();
    let statuses = [];
    
    if (fate < 0.3) { // 30% тикетов - только открыты и забыты
        statuses = ['open'];
    } else if (fate < 0.6) { // 30% - открыты и в работе
        statuses = ['open', 'in_progress'];
    } else if (fate < 0.85) { // 25% - открыты, в работе, решены
        statuses = ['open', 'in_progress', 'resolved'];
    } else { // 15% - полный цикл до закрытия
        statuses = ['open', 'in_progress', 'resolved', 'closed'];
    }
    
    const messages = [];
    let lastTimestamp = new Date(created);
    
    messages.push({
        sender: "user",
        message: randomElement(userMessages),
        timestamp: lastTimestamp.toISOString()
    });
    
    // Добавляем ответы поддержки в зависимости от количества статусов
    const numSupportResponses = statuses.length - 1; 
    
    for (let resp = 0; resp < numSupportResponses; resp++) {
        // Добавляем задержку между сообщениями
        const delay = randomInt(30, 240) * 60000; 
        lastTimestamp = new Date(lastTimestamp.getTime() + delay);
        
        messages.push({
            sender: "support",
            message: randomElement(supportMessages),
            timestamp: lastTimestamp.toISOString()
        });
        
        if (resp < numSupportResponses - 1) {
            const userDelay = randomInt(15, 120) * 60000; 
            lastTimestamp = new Date(lastTimestamp.getTime() + userDelay);
            
            messages.push({
                sender: "user",
                message: "Спасибо, я понял. Проверю.",
                timestamp: lastTimestamp.toISOString()
            });
        }
    }
    
    for (let s = 0; s < statuses.length; s++) {
        const status = statuses[s];
        
        // Для каждого статуса определяем время обновления
        let updatedAt;
        if (s === 0) {
            updatedAt = null;
        } else {
            const messageIndex = s * 2 - 1;
            if (messageIndex < messages.length) {
                updatedAt = messages[messageIndex].timestamp;
            } else {
                updatedAt = messages[messages.length - 1].timestamp;
            }
        }
        
        if (status === 'closed') {
            updatedAt = messages[messages.length - 1].timestamp;
        }
        
        allTickets.push({
            ticket_id: ticketId,
            user_id: userId,
            status: status,
            issue_type: issueType,
            messages: messages.slice(0, s * 2 + 1), 
            created_at: created.toISOString(),
            updated_at: updatedAt
        });
    }
}

allTickets.sort(() => Math.random() - 0.5);
db.SupportTickets.insertMany(allTickets);

// -------------------------------------------------------------------------------

db.UserRecommendations.drop();

const recommendations = [];
for (let i = 1; i <= 300; i++) {
    recommendations.push({
        user_id: `user_${i}`,
        recommended_products: [randomElement(PRODUCTS), randomElement(PRODUCTS), randomElement(PRODUCTS)],
        last_updated: randomDate(new Date('2026-02-01'), END_DATE).toISOString()
    });
}
db.UserRecommendations.insertMany(recommendations);

// -------------------------------------------------------------------------------

db.ModerationQueue.drop();

const reviews = [];
const reviewTexts = [
    "Отличный товар!",
    "Неплохо, но дорого",
    "Не подошло",
    "Рекомендую",
    "Так себе"
];

for (let i = 1; i <= 600; i++) {
    reviews.push({
        review_id: `rev_${String(i).padStart(3, '0')}`,
        user_id: `user_${randomInt(1, 300)}`,
        product_id: randomElement(PRODUCTS),
        review_text: randomElement(reviewTexts),
        rating: randomInt(1, 5),
        moderation_status: randomElement(MODERATION_STATUSES),
        flags: Math.random() > 0.7 ? [randomElement(FLAGS)] : [],
        submitted_at: randomDate(START_DATE, END_DATE).toISOString()
    });
}
db.ModerationQueue.insertMany(reviews);
