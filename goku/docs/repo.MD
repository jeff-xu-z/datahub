# Creation of this repo

- A fork of https://github.com/datahub-project/datahub was made in github.com.
- Clone the datahub fork: ```git clone https://github.com/jeff-xu-z/datahub.git```.
- In the local repo:

```
git remote add upstream https://github.com/datahub-project/datahub.git
git checkout master
git merge upstream/master
```

- Create **pharos-goku-datahub** repo in GHE.
- Add the GHE repo: ```git remote add pharos-goku-datahub https://ghe.megaleo.com/dpoe/pharos-goku-datahub.git```.
- Push to the GHE repo: ```git push -u pharos-goku-datahub master```

# Branching

- We should reserve the **master** branch to sync open source datahub changes.
- We will use **goku** as our internal master branch.
- Any development work should use branch created from goku and PR back to goku branch.
