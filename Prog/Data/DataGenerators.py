import random as r
import string

USERMAX = 20

first_names = [
    "James","John","Robert","Michael","William","David","Richard","Charles",
    "Joseph","Thomas","Mary","Patricia","Linda","Barbara","Elizabeth",
    "Jennifer","Maria","Susan","Margaret","Dorothy"
]

last_names = [
    "Smith","Johnson","Williams","Jones","Brown","Davis","Miller","Wilson",
    "Moore","Taylor","Anderson","Thomas","Jackson","White","Harris","Martin",
    "Thompson","Martinez","Garcia","Robinson"
]

orgs = {
    "CNN": "Web Development",
    "Google": "Search Optimization",
    "Facebook": "Social Networking",
    "Twitter": "Social Media",
    "Amazon": "E-commerce",
    "Microsoft": "Video Games",
    "Apple": "Mobile Technology",
    "LG": "Cell Phones",
    "IBM": "Computer Research",
    "Asus": "Laptops",
    "Instagram": "Photography",
    "Chase": "Banking",
    "Intel": "Hardware Engineering",
    "Bose": "Audio Engineering"
}

skills = [
    "Programming","Accounting","Databases","Hardware","Public Relations",
    "Data Analysis","Machine Learning","Marketing","Networking","Sales",
    "Electrical Engineering","Communication","Management","Web Development",
    "Mobile Development","Business"
]

interests = [
    "Video Games","Surfing","Physical Fitness","Movies","TV","Snowboarding",
    "Card Games", "Wine", "Travelling", "Food and Cooking", "Photography",
    "Football", "Soccer", "Painting", "History", "Reading", "Museums", "Drawing"
]


def generate_all():
    generate_names()
    generate_organizations()
    generate_projects()
    generate_skills()
    generate_interests()
    generate_distances()


def generate_names():
    """User_id, first name, last name"""
    name_file = open("names.csv", "w")
    for i in range(1,USERMAX+1):
        name_file.write(i.__str__())
        name_file.write(',')
        name_file.write(r.choice(first_names))
        name_file.write(',')
        name_file.write(r.choice(last_names))
        name_file.write('\n')
    name_file.close()


def generate_organizations():
    """User_id, organization, organization type"""
    orgs_file = open("orgs.csv", "w")
    for i in range(1, USERMAX+1):
        org = r.choice(orgs.keys())
        orgs_file.write(i.__str__())
        orgs_file.write(',')
        orgs_file.write(org)
        orgs_file.write(',')
        orgs_file.write(orgs[org])
        orgs_file.write('\n')
    orgs_file.close()


def generate_projects():
    """User_id, project"""
    proj_file = open("proj.csv", "w")
    for i in range(1, USERMAX+1):
        for j in range(r.randint(3, 10)):
            proj_file.write(i.__str__())
            proj_file.write(',')
            proj_file.write(r.choice(string.letters).upper())
            proj_file.write('\n')


def generate_skills():
    """User_id, Skill, Skill level"""
    skills_file = open("skills.csv", "w")
    for i in range(1, USERMAX+1):
        l = list()
        for j in range(r.randint(3, 10)):
            x = r.choice(skills)
            if x not in l:
                skills_file.write(i.__str__())
                skills_file.write(',')
                skills_file.write(x)
                skills_file.write(',')
                skills_file.write(r.randint(1, 10).__str__())
                skills_file.write('\n')
                l.append(x)


def generate_interests():
    """User_id, Interest, Interest level"""
    interests_file = open("interests.csv", "w")
    for i in range(1, USERMAX+1):
        l = list()
        for j in range(r.randint(3, 10)):
            x = r.choice(interests)
            if x not in l:
                interests_file.write(i.__str__())
                interests_file.write(',')
                interests_file.write(x)
                interests_file.write(',')
                interests_file.write(r.randint(1, 10).__str__())
                interests_file.write('\n')
                l.append(x)


def generate_distances():
    distance_file = open("distance.csv", "w")
    key_list = orgs.keys()

    for i in orgs.keys():
        key_list.remove(i)
        for j in key_list:
            distance_file.write(i)
            distance_file.write(',')
            distance_file.write(j)
            distance_file.write(',')
            distance_file.write(r.randint(1, 40).__str__())
            distance_file.write('\n')


generate_all()