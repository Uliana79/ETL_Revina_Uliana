CREATE DATABASE service;
\c service;

DROP TABLE IF EXISTS Users CASCADE;
CREATE TABLE Users (
    user_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255) NOT NULL,
    name VARCHAR(255) NOT NULL,
    registration_date TIMESTAMP NOT NULL,
    last_active TIMESTAMP NOT NULL,
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_users_email ON Users(email);
CREATE INDEX idx_users_registration_date ON Users(registration_date);

-------------------------------------------------------------------------------

DROP TABLE IF EXISTS UserSessions CASCADE;
CREATE TABLE UserSessions (
    session_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    device VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CHECK (end_time >= start_time)
);
CREATE INDEX idx_sessions_user_id ON UserSessions(user_id);
CREATE INDEX idx_sessions_start_time ON UserSessions(start_time);
CREATE INDEX idx_sessions_end_time ON UserSessions(end_time);

DROP TABLE IF EXISTS SessionPages CASCADE;
CREATE TABLE SessionPages (
    session_id VARCHAR(50) NOT NULL REFERENCES UserSessions(session_id) ON DELETE CASCADE,
    page_url VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, page_url)
);
CREATE INDEX idx_session_pages_session_id ON SessionPages(session_id);

DROP TABLE IF EXISTS SessionActions CASCADE;
CREATE TABLE SessionActions (
    session_id VARCHAR(50) NOT NULL REFERENCES UserSessions(session_id) ON DELETE CASCADE,
    action_name VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (session_id, action_name)
);
CREATE INDEX idx_session_actions_session_id ON SessionActions(session_id);

-------------------------------------------------------------------------------

DROP TABLE IF EXISTS EventLogs CASCADE;
CREATE TABLE EventLogs (
    event_id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    details JSONB DEFAULT '{}',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_events_timestamp ON EventLogs(timestamp);
CREATE INDEX idx_events_type ON EventLogs(event_type);

-------------------------------------------------------------------------------

DROP TABLE IF EXISTS SupportTickets CASCADE;
CREATE TABLE SupportTickets (
    ticket_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    user_id VARCHAR(50) NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
    issue_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP,
    PRIMARY KEY (ticket_id, status)
);
CREATE INDEX idx_tickets_user_id ON SupportTickets(user_id);
CREATE INDEX idx_tickets_status ON SupportTickets(status);
CREATE INDEX idx_tickets_issue_type ON SupportTickets(issue_type);
CREATE INDEX idx_tickets_created_at ON SupportTickets(created_at);

DROP TABLE IF EXISTS TicketMessages CASCADE;
CREATE TABLE TicketMessages (
    ticket_id VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    sender VARCHAR(20) NOT NULL CHECK (sender IN ('user', 'support')),
    message TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (ticket_id, status) REFERENCES SupportTickets(ticket_id, status) ON DELETE CASCADE
);
CREATE INDEX idx_ticket_messages_ticket_id ON TicketMessages(ticket_id);
CREATE INDEX idx_ticket_messages_timestamp ON TicketMessages(timestamp);

-------------------------------------------------------------------------------

DROP TABLE IF EXISTS UserRecommendations CASCADE;
CREATE TABLE UserRecommendations (
    user_id VARCHAR(50) PRIMARY KEY REFERENCES Users(user_id) ON DELETE CASCADE,
    recommended_products TEXT[],
    last_updated TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_recommended_products_user_id ON UserRecommendations(user_id);

-------------------------------------------------------------------------------

DROP TABLE IF EXISTS ModerationQueue CASCADE;
CREATE TABLE ModerationQueue (
    review_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) NOT NULL REFERENCES Users(user_id) ON DELETE CASCADE,
    product_id VARCHAR(50) NOT NULL,
    review_text TEXT NOT NULL,
    rating INTEGER NOT NULL CHECK (rating >= 1 AND rating <= 5),
    moderation_status VARCHAR(50) NOT NULL,
    flags TEXT[],
    submitted_at TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_moderation_user_id ON ModerationQueue(user_id);
CREATE INDEX idx_moderation_product_id ON ModerationQueue(product_id);
CREATE INDEX idx_moderation_status ON ModerationQueue(moderation_status);
CREATE INDEX idx_moderation_submitted_at ON ModerationQueue(submitted_at);
