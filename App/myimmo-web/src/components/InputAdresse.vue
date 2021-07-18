<template>
    <v-row
        align="center"
        justify="center"
      >
        <v-col cols="12">
            <h1 align="center">Estimer votre bien immobilier avec MyImmo !</h1>
        </v-col>
        <v-col cols="8">
          <v-text-field
            v-model="valueAdr"
            :loading="loading"
            :disabled="disabled"
            clearable
            outlined
            label="Adresse de votre bien"
            placeholder="242 Rue du Faubourg Saint-Antoine, 75012 Paris"
            append-icon="mdi-map-marker"
          ></v-text-field>

          <v-card
            class="mx-auto"
            min-width="300"
            min-height="250"

            tile
          >
          <v-list dense>
          <v-subheader>Selectionner une adresse</v-subheader>
          <v-list-item-group
            color="primary"
            v-model="selectedItem"
            
          >
            <v-list-item
              v-for="(item, i) in adresse"
              :key="i"
            >
              <v-list-item-content>
                <v-list-item-title v-text="item.properties.label"></v-list-item-title>
              </v-list-item-content>
            </v-list-item>
          </v-list-item-group>
        </v-list>
        </v-card>
        </v-col>
        <v-col cols="8">
          <v-text-field
            v-model="surface"
            clearable
            outlined
            label="Surface financiere m²"
            placeholder="250"
          ></v-text-field>
        </v-col>
        <v-col cols="8" class="text-center">
            <EstimationDialogue
            :infoAdresse="adresse[selectedItem]"
            :surface="surface"
            ></EstimationDialogue>
        </v-col>
    </v-row>
</template>

<script>
  import EstimationDialogue from '../components/EstimationDialogue.vue'
  export default {
    name: 'InputAdresse',
    components: {
      EstimationDialogue,
    },
    data: () => ({
      valueAdr: null,
      selectedItem: null,
      adresse: [],
      loading: false,
      disabled: false,
      surface: null
    }),
    methods: {
    },
    watch: {
        valueAdr: async function (newRep) {
          if (this.loading || this.valueAdr == null || this.valueAdr == "") return
          this.loading = true
          this.adresse = []
          this.disabled = false
          this.selectedItem = null
          axios.get(`https://api-adresse.data.gouv.fr/search/?q=${newRep}&autocomplete=1`)
            .then((result) => {
                var dataHtpp = result.data
                dataHtpp.features.forEach(element => {
                  this.adresse.push(element)
                });
                this.loading = false
            })
            .catch(function () {
                console.log('FAILURE!!')
            })
        },
        selectedItem : function (newRep) {
          if(newRep == undefined || newRep == null){
            this.disabled = false
          } else {
            this.disabled = true
          }
          console.log(newRep)
          console.log(this.adresse[newRep])
        }
    }
  }
</script>