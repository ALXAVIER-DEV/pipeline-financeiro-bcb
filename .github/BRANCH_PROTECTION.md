# Proteção de branches

O fluxo esperado é:

1. pushes em `feature/**` criam uma pull request para `develop`;
2. merges em `develop` criam ou atualizam uma pull request para `main`;
3. a pull request para `main` só pode ser integrada após o CI e a aprovação
   do tech lead.

## Configuração necessária no GitHub

Crie um ruleset para `main` em **Settings > Rules > Rulesets** com:

- bloquear pushes diretos;
- exigir pull request antes do merge;
- exigir pelo menos uma aprovação;
- exigir aprovação de Code Owners;
- invalidar aprovações quando novos commits forem enviados;
- exigir resolução de todas as conversas;
- exigir o status check do job `lint-and-test`;
- impedir bypass, exceto para administradores de emergência definidos pela
  organização.

Adicione também `.github/CODEOWNERS` com o usuário ou time real do tech lead:

```text
* @organizacao/time-tech-leads
```

Para `develop`, recomenda-se outro ruleset que bloqueie pushes diretos e exija
o status check `lint-and-test`. A aprovação humana nessa etapa pode ser
opcional, conforme a política do time.

Em **Settings > Actions > General > Workflow permissions**, habilite
**Allow GitHub Actions to create and approve pull requests**. A automação usa
essa permissão apenas para criar as pull requests; ela não as aprova.
