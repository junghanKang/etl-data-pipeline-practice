db.createUser(
    {
        user: "deteam",
        pwd: "1234",
        roles: [
            {
                role: "readWrite",
                db: "healthcare"
            }
        ]
    }
);
