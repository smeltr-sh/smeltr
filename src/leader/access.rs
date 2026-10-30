//! What an account may do, per area.
//!
//! An area is `shared` or one worker group: the scope every config read and write already names. An account holds
//! one [`Level`] in an area, and each level includes the ones below it, from the roles it names: a role is a name
//! and the level it gives in each area. An OIDC account names the roles its provider's groups name; a local or key
//! account names the roles an administrator gave it. An account is an administrator by its own flag, which only a
//! local or key account can be given, or by holding a role marked administrator.
//!
//! The leader decides this on every request, from the names as they stand, so a change to a role reaches everyone
//! holding it on their next request and a change to someone's groups at their next sign-in.
//!
//! A person's agent acts for them with [`AgentPermissions`]: a part of what they hold, never more, so what it may
//! do is the lower of the two in each area and shrinks with them.

use std::collections::BTreeMap;

/// The area every group draws on.
pub const SHARED: &str = "shared";

/// What a person may do in one area. Ordered: each level includes every level before it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Level {
    /// Reads the area's configuration.
    Viewer,
    /// Saves to it on a branch, and captures, previews and reads its samples, which hold production records.
    Editor,
    /// Approves and deploys a change to it.
    Deployer,
}

impl Level {
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Viewer => "viewer",
            Level::Editor => "editor",
            Level::Deployer => "deployer",
        }
    }

    pub fn parse(s: &str) -> Option<Level> {
        match s {
            "viewer" => Some(Level::Viewer),
            "editor" => Some(Level::Editor),
            "deployer" => Some(Level::Deployer),
            _ => None,
        }
    }
}

/// One provider group and what holding it gives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Role {
    /// The group exactly as the provider's groups claim names it.
    pub name: String,
    /// Holds everything, everywhere, as a local account does. `areas` is then beside the point.
    pub administrator: bool,
    pub areas: BTreeMap<String, Level>,
}

/// What one person holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Access {
    Administrator,
    /// The highest level held in each area. An area not named is one the person cannot see.
    Areas(BTreeMap<String, Level>),
    /// A person's agent: what `held` holds, no higher in any area than `permissions` give. Never an administrator.
    Agent {
        held: Box<Access>,
        permissions: AgentPermissions,
    },
}

/// Marks a request the leader's agent sends for the person whose session it carries, so the gate judges it by
/// that person's [`AgentPermissions`]. A request extension, which only the leader itself can attach: nothing on the wire
/// becomes one.
#[derive(Clone, Copy, Debug)]
pub struct ByAgent;

/// How much of what a person holds their agent may use.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase", tag = "kind")]
pub enum AgentPermissions {
    /// Everything the person holds, administrator included.
    Everything,
    /// At most `default` in every area the person holds, or the level `areas` names for that area.
    Upto {
        default: Level,
        #[serde(default)]
        areas: BTreeMap<String, Level>,
    },
}

impl Default for AgentPermissions {
    /// What a person's agent may use until they say otherwise: reading what they read.
    fn default() -> AgentPermissions {
        AgentPermissions::Upto {
            default: Level::Viewer,
            areas: BTreeMap::new(),
        }
    }
}

impl AgentPermissions {
    /// The highest level these permissions let an agent use in `area`. `None` for all of it.
    fn cap(&self, area: &str) -> Option<Level> {
        match self {
            AgentPermissions::Everything => None,
            AgentPermissions::Upto { default, areas } => Some(areas.get(area).copied().unwrap_or(*default)),
        }
    }

    /// The highest level these permissions give in any area. `None` for all of it.
    fn highest(&self) -> Option<Level> {
        match self {
            AgentPermissions::Everything => None,
            AgentPermissions::Upto { default, areas } => Some(areas.values().copied().fold(*default, Level::max)),
        }
    }
}

impl Access {
    /// What an account holds: everything when its own flag says so, else what the roles it names give, out of
    /// the roles that exist.
    pub fn of(administrator: bool, named: &[String], roles: &[Role]) -> Access {
        if administrator {
            return Access::Administrator;
        }
        let held = roles.iter().filter(|role| named.contains(&role.name));
        let mut areas = BTreeMap::new();
        for role in held {
            if role.administrator {
                return Access::Administrator;
            }
            for (area, level) in &role.areas {
                let at = areas.entry(area.clone()).or_insert(*level);
                *at = (*at).max(*level);
            }
        }
        Access::Areas(areas)
    }

    /// What `self`'s agent holds, given the agent permissions `self` gives it.
    pub fn for_agent(self, permissions: AgentPermissions) -> Access {
        match permissions {
            AgentPermissions::Everything => self,
            permissions => Access::Agent {
                held: Box::new(self),
                permissions,
            },
        }
    }

    pub fn is_administrator(&self) -> bool {
        matches!(self, Access::Administrator)
    }

    /// The level held in `area`, if any.
    ///
    /// Anyone holding any area reads `shared` as well: a group's configuration is read merged with it, and a
    /// group's routes name shared pipelines, so a group cannot be read without it.
    pub fn level(&self, area: &str) -> Option<Level> {
        match self {
            Access::Administrator => Some(Level::Deployer),
            Access::Areas(areas) => {
                let named = areas.get(area).copied();
                if area == SHARED && !areas.is_empty() {
                    return Some(named.unwrap_or(Level::Viewer));
                }
                named
            }
            Access::Agent { held, permissions } => {
                let level = held.level(area)?;
                Some(permissions.cap(area).map_or(level, |cap| level.min(cap)))
            }
        }
    }

    /// Whether `need` is held in `area`.
    pub fn allows(&self, area: &str, need: Level) -> bool {
        self.level(area).is_some_and(|held| held >= need)
    }

    /// Whether `need` is held in at least one area.
    pub fn somewhere(&self, need: Level) -> bool {
        match self {
            Access::Administrator => true,
            Access::Areas(areas) => areas.values().any(|held| *held >= need),
            Access::Agent { held, permissions } => match held.as_ref() {
                // Every area is held, so the permissions' own highest level is held somewhere.
                Access::Administrator => permissions.highest().is_none_or(|highest| highest >= need),
                Access::Areas(areas) => areas.keys().any(|area| self.allows(area, need)),
                Access::Agent { .. } => held.somewhere(need) && permissions.highest().is_none_or(|h| h >= need),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn role(name: &str, areas: &[(&str, Level)]) -> Role {
        Role {
            name: name.into(),
            administrator: false,
            areas: areas.iter().map(|(a, l)| (a.to_string(), *l)).collect(),
        }
    }

    fn groups(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    /// Model, point 3: an account's own flag holds everything, whatever its roles say.
    #[test]
    fn an_administrators_flag_holds_everything_whatever_the_roles_say() {
        let roles = [role("smeltr-payments", &[("payments", Level::Viewer)])];
        assert_eq!(
            Access::of(true, &groups(&["smeltr-payments"]), &roles),
            Access::Administrator
        );
    }

    /// Model, point 3: an account without the flag holds only what its roles give, however it signs in.
    #[test]
    fn an_account_without_the_flag_holds_only_what_its_roles_give() {
        let roles = [role("smeltr-payments", &[("payments", Level::Viewer)])];
        let access = Access::of(false, &groups(&["smeltr-payments"]), &roles);
        assert!(!access.is_administrator());
        assert_eq!(access.level("payments"), Some(Level::Viewer));
        assert_eq!(Access::of(false, &[], &roles), Access::Areas(BTreeMap::new()));
    }

    #[test]
    fn an_account_holds_the_highest_level_its_roles_give_in_each_area() {
        let roles = [
            role(
                "smeltr-storefront",
                &[("storefront", Level::Deployer), ("search", Level::Editor)],
            ),
            role(
                "smeltr-oncall",
                &[("storefront", Level::Viewer), ("payments", Level::Viewer)],
            ),
            role("smeltr-payments", &[("payments", Level::Deployer)]),
        ];
        let access = Access::of(false, &groups(&["smeltr-storefront", "smeltr-oncall"]), &roles);
        assert_eq!(access.level("storefront"), Some(Level::Deployer));
        assert_eq!(access.level("search"), Some(Level::Editor));
        assert_eq!(access.level("payments"), Some(Level::Viewer));
        assert_eq!(access.level("default"), None);
    }

    #[test]
    fn a_role_marked_administrator_holds_everything() {
        let mut admins = role("smeltr-admins", &[]);
        admins.administrator = true;
        let access = Access::of(false, &groups(&["smeltr-admins"]), &[admins]);
        assert!(access.is_administrator());
        assert!(access.allows("anything", Level::Deployer));
    }

    #[test]
    fn a_group_no_role_names_gives_nothing() {
        let roles = [role("smeltr-payments", &[("payments", Level::Deployer)])];
        let access = Access::of(false, &groups(&["everyone"]), &roles);
        assert_eq!(access, Access::Areas(BTreeMap::new()));
        assert!(!access.somewhere(Level::Viewer));
        assert_eq!(access.level(SHARED), None);
    }

    /// A group is read merged with shared, so holding any area reads shared too -- and no more than reads it.
    #[test]
    fn holding_any_area_reads_shared() {
        let roles = [role("smeltr-search", &[("search", Level::Deployer)])];
        let access = Access::of(false, &groups(&["smeltr-search"]), &roles);
        assert!(access.allows(SHARED, Level::Viewer));
        assert!(!access.allows(SHARED, Level::Editor));
    }

    fn upto(default: Level, areas: &[(&str, Level)]) -> AgentPermissions {
        AgentPermissions::Upto {
            default,
            areas: areas.iter().map(|(a, l)| (a.to_string(), *l)).collect(),
        }
    }

    #[test]
    fn an_agent_holds_the_lower_of_its_person_and_its_permissions() {
        let roles = [role("r", &[("payments", Level::Deployer), ("search", Level::Viewer)])];
        let person = Access::of(false, &groups(&["r"]), &roles);
        let agent = person.for_agent(upto(
            Level::Viewer,
            &[("payments", Level::Editor), ("search", Level::Deployer)],
        ));
        assert_eq!(agent.level("payments"), Some(Level::Editor));
        assert_eq!(
            agent.level("search"),
            Some(Level::Viewer),
            "agent permissions never raise their person"
        );
        assert_eq!(
            agent.level("default"),
            None,
            "nor reaches an area its person cannot see"
        );
        assert_eq!(agent.level(SHARED), Some(Level::Viewer));
        assert!(agent.somewhere(Level::Editor));
        assert!(!agent.somewhere(Level::Deployer));
        assert!(!agent.is_administrator());
    }

    #[test]
    fn an_administrators_agent_is_no_administrator_unless_given_everything() {
        let agent = Access::Administrator.for_agent(AgentPermissions::default());
        assert!(!agent.is_administrator());
        assert!(agent.allows("anything", Level::Viewer));
        assert!(!agent.allows("anything", Level::Editor));
        assert!(!agent.somewhere(Level::Editor));
        assert!(
            Access::Administrator
                .for_agent(upto(Level::Viewer, &[("payments", Level::Deployer)]))
                .somewhere(Level::Deployer)
        );
        assert!(
            Access::Administrator
                .for_agent(AgentPermissions::Everything)
                .is_administrator()
        );
    }

    #[test]
    fn each_level_includes_the_ones_below_it() {
        let roles = [role("r", &[("payments", Level::Editor)])];
        let access = Access::of(false, &groups(&["r"]), &roles);
        assert!(access.allows("payments", Level::Viewer));
        assert!(access.allows("payments", Level::Editor));
        assert!(!access.allows("payments", Level::Deployer));
        assert!(access.somewhere(Level::Editor));
        assert!(!access.somewhere(Level::Deployer));
    }
}
