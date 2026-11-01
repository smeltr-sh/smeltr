//! The company every reference log is about: its people and its sites. The
//! firewalls see its users and networks, the bastions its accounts, and the
//! lookup tables that enrich them are built from these same lists, so a
//! join in a pipeline finds what the logs name.

/// Given names and family names. Every pairing is one person, so the
/// directory is as wide as the benchmark's lookup wants and every name in
/// it reads as a person's.
const GIVEN: &[&str] = &[
    "Jia", "Marta", "Arjun", "Sade", "Linh", "Robert", "Tove", "Hiro", "Dana", "Elena", "Kevin", "Filip", "Yara",
    "Beth", "Nadia", "Carlos", "Paul", "Ivy", "Owen", "Vera", "Amir", "Chloe", "Diego", "Freya", "Gabriel", "Hana",
    "Isaac", "Julia", "Kofi", "Leila", "Mateo", "Noor",
];
const FAMILY: &[&str] = &[
    "Chen",
    "Garcia",
    "Patel",
    "Okafor",
    "Nguyen",
    "Kowalski",
    "Johansson",
    "Sato",
    "Murphy",
    "Romero",
    "Brown",
    "Novak",
    "Silva",
    "Wright",
    "Hassan",
    "Lopez",
    "Mueller",
    "Kim",
    "Mensah",
    "Rossi",
    "Haddad",
    "Dubois",
    "Alvarez",
    "Larsen",
    "Costa",
    "Tanaka",
    "Levi",
    "Fischer",
    "Asante",
    "Karimi",
    "Herrera",
    "Shah",
];

/// Departments, each with the title its people hold.
const DEPARTMENTS: &[(&str, &str)] = &[
    ("Engineering", "Software Engineer"),
    ("Finance", "Analyst"),
    ("Sales", "Account Executive"),
    ("Support", "Support Engineer"),
    ("IT Operations", "Systems Administrator"),
    ("Marketing", "Marketing Manager"),
    ("Legal", "Counsel"),
    ("Security", "Security Engineer"),
    ("HR", "HR Partner"),
    ("Executive", "Director"),
];

/// How many people the directory holds.
pub const PEOPLE: usize = GIVEN.len() * FAMILY.len();

/// One person in the directory.
pub struct Person {
    /// The login, as `jia.chen`. A Windows domain prefixes it: `acme\jia.chen`.
    pub login: String,
    pub full_name: String,
    pub department: &'static str,
    pub title: &'static str,
}

/// Person `n`, for any `n`: past [`PEOPLE`] the directory wraps.
pub fn person(n: u64) -> Person {
    let n = n as usize % PEOPLE;
    let (given, family) = (GIVEN[n % GIVEN.len()], FAMILY[n / GIVEN.len()]);
    let (department, title) = DEPARTMENTS[(n + n / GIVEN.len()) % DEPARTMENTS.len()];
    Person {
        login: format!("{}.{}", given.to_lowercase(), family.to_lowercase()),
        full_name: format!("{given} {family}"),
        department,
        title,
    }
}

/// The directory as its owner exports it: one row per person.
pub fn directory() -> (Vec<&'static str>, Vec<Vec<String>>) {
    let rows = (0..PEOPLE as u64)
        .map(|n| {
            let p = person(n);
            vec![
                format!("acme\\{}", p.login),
                p.department.to_string(),
                p.full_name,
                p.title.to_string(),
            ]
        })
        .collect();
    (vec!["account", "department", "full_name", "title"], rows)
}

/// The company's sites: a short name, its city, and whether its networks are
/// restricted.
pub const SITES: &[(&str, &str)] = &[
    ("nyc", "New York"),
    ("lon", "London"),
    ("fra", "Frankfurt"),
    ("sin", "Singapore"),
    ("sfo", "San Francisco"),
    ("dub", "Dublin"),
    ("tor", "Toronto"),
    ("syd", "Sydney"),
];

/// How many `10.<n>.0.0/16` networks the company runs. Network `n` is at
/// site `n % SITES.len()`.
pub const NETWORKS: u64 = 64;

/// The site network `n` is at.
pub fn site_of(n: u64) -> (&'static str, &'static str) {
    SITES[(n % SITES.len() as u64) as usize]
}

/// Every network, with its site, city, tier and business unit.
pub fn networks() -> (Vec<&'static str>, Vec<Vec<String>>) {
    let rows = (0..NETWORKS)
        .map(|n| {
            let (site, city) = site_of(n);
            vec![
                format!("10.{n}.0.0/16"),
                site.to_string(),
                city.to_string(),
                if n % 4 == 0 { "restricted" } else { "general" }.to_string(),
                format!("bu-{:02}", n % 12),
            ]
        })
        .collect();
    (vec!["network", "site", "city", "tier", "business_unit"], rows)
}

/// The [`PUBLIC`] address password spraying comes from, whichever feed sees it.
pub const SPRAYER: usize = 11;

/// Internet addresses from the ranges MaxMind's GeoIP2 test databases
/// answer for, with the country each is in.
pub const PUBLIC: &[(&str, &str)] = &[
    ("81.2.69.142", "United Kingdom"),
    ("81.2.69.160", "United Kingdom"),
    ("81.2.69.193", "United Kingdom"),
    ("89.160.20.112", "Sweden"),
    ("89.160.20.128", "Sweden"),
    ("89.160.20.156", "Sweden"),
    ("175.16.199.1", "China"),
    ("175.16.199.24", "China"),
    ("216.160.83.56", "United States"),
    ("216.160.83.58", "United States"),
    ("216.160.83.61", "United States"),
    ("67.43.156.13", "Bhutan"),
    ("67.43.156.44", "Bhutan"),
    ("2.125.160.216", "United Kingdom"),
];
