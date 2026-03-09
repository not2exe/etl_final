DROP TABLE IF EXISTS user_sessions CASCADE;
CREATE TABLE user_sessions (
    session_id      VARCHAR(36) PRIMARY KEY,
    user_id         VARCHAR(20) NOT NULL,
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP NOT NULL,
    pages_visited   JSONB NOT NULL,
    device          JSONB NOT NULL,
    actions         JSONB NOT NULL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS event_logs CASCADE;
CREATE TABLE event_logs (
    event_id        VARCHAR(36) PRIMARY KEY,
    user_id         VARCHAR(20) NOT NULL,
    timestamp       TIMESTAMP NOT NULL,
    event_type      VARCHAR(50) NOT NULL,
    details         JSONB NOT NULL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS support_tickets CASCADE;
CREATE TABLE support_tickets (
    ticket_id       VARCHAR(36) PRIMARY KEY,
    user_id         VARCHAR(20) NOT NULL,
    created_at      TIMESTAMP NOT NULL,
    updated_at      TIMESTAMP NOT NULL,
    status          VARCHAR(20) NOT NULL,
    issue_type      VARCHAR(50) NOT NULL,
    messages        JSONB NOT NULL,
    loaded_at       TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS user_recommendations CASCADE;
CREATE TABLE user_recommendations (
    user_id                 VARCHAR(20) PRIMARY KEY,
    recommended_products    JSONB NOT NULL,
    last_updated            TIMESTAMP NOT NULL,
    loaded_at               TIMESTAMP DEFAULT NOW()
);

DROP TABLE IF EXISTS moderation_queue CASCADE;
CREATE TABLE moderation_queue (
    review_id           VARCHAR(36) PRIMARY KEY,
    user_id             VARCHAR(20) NOT NULL,
    product_id          VARCHAR(20) NOT NULL,
    review_text         TEXT NOT NULL,
    rating              INTEGER NOT NULL,
    moderation_status   VARCHAR(20) NOT NULL,
    flags               JSONB NOT NULL,
    created_at          TIMESTAMP NOT NULL,
    loaded_at           TIMESTAMP DEFAULT NOW()
);
